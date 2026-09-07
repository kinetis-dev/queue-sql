<?php

declare(strict_types=1);

/**
 * Real-backend coverage for what SqlQueue's committed PHPUnit tests
 * cannot prove against a fake: FOR UPDATE SKIP LOCKED, priority-queue
 * cycling, the visibility-timeout reclaim, and that the server itself
 * answers a settlement fenced to a superseded reservation with an
 * affected-row count of zero.
 */

require __DIR__ . '/../vendor/autoload.php';

use Kinetis\Persistence\Contract\MysqlLink;
use Kinetis\Persistence\Driver\MysqliAsyncClient;
use Kinetis\Queue\Exception\InvalidQueueArgumentException;
use Kinetis\Queue\Exception\MalformedJobSettledException;
use Kinetis\Queue\Exception\StaleJobHandleException;
use Kinetis\Queue\JobSettlement;
use Kinetis\Queue\Job;
use Kinetis\Queue\QueuedJob;
use Kinetis\Queue\QueueInterface;
use Kinetis\QueueSql\SqlQueue;

function check(string $label, bool $condition): void
{
    echo ($condition ? "OK   " : "FAIL ") . $label . "\n";

    if (!$condition) {
        exit(1);
    }
}

final readonly class IntegrationTestJob implements Job
{
    public function __construct(
        public string $message,
    ) {}

    public function handle(): void
    {
    }
}

function runQueueChecks(string $backend, QueueInterface $queue): void
{
    echo "=== {$backend} ===\n";

    $queue->push(new IntegrationTestJob('hello'));
    $popped = $queue->pop(timeoutSeconds: 5);
    check("{$backend}: pop() returns the pushed job", $popped instanceof QueuedJob);
    check("{$backend}: job data round-trips correctly", $popped?->args['message'] === 'hello');
    check("{$backend}: attempts is 1 on first pop", $popped?->attempts === 1);

    $queue->ack($popped);
    check("{$backend}: nothing left after ack()", $queue->pop(timeoutSeconds: 1) === null);

    // release() increments attempts and makes the job available again.
    $queue->push(new IntegrationTestJob('retry-me'), maxAttempts: 3);
    $first = $queue->pop(timeoutSeconds: 5);
    $queue->release($first);
    $second = $queue->pop(timeoutSeconds: 5);
    check("{$backend}: released job comes back with attempts incremented", $second?->attempts === 2);
    $queue->ack($second);

    // fail() removes the job permanently.
    $queue->push(new IntegrationTestJob('doomed'));
    $doomed = $queue->pop(timeoutSeconds: 5);
    $queue->fail($doomed);
    check("{$backend}: nothing left after fail()", $queue->pop(timeoutSeconds: 1) === null);

    // Priority queues: a higher-priority queue is checked before the default one.
    $queue->push(new IntegrationTestJob('low-priority'), queue: 'default');
    $queue->push(new IntegrationTestJob('high-priority'), queue: 'high');

    $priorityPop = $queue->pop(timeoutSeconds: 5, queues: ['high', 'default']);
    check("{$backend}: the high-priority queue is checked first", $priorityPop?->args['message'] === 'high-priority');
    $queue->ack($priorityPop);

    $remaining = $queue->pop(timeoutSeconds: 5, queues: ['high', 'default']);
    check("{$backend}: falls through to the default queue next", $remaining?->args['message'] === 'low-priority');
    $queue->ack($remaining);

    // The deadline/priority-sweep contract is validated the same way
    // across every backend, via Kinetis\Queue\QueueContract — SqlQueue's
    // own single combined priority query never even needed the
    // per-queue-loop fix RedisQueue/SqsQueue/RabbitMqQueue did, but it
    // shares this input validation with all of them.
    try {
        $queue->pop(timeoutSeconds: -1);
        check("{$backend}: a negative timeout is rejected", false);
    } catch (InvalidQueueArgumentException) {
        check("{$backend}: a negative timeout is rejected", true);
    }

    try {
        $queue->pop(queues: ['default', '']);
        check("{$backend}: an empty queue name is rejected", false);
    } catch (InvalidQueueArgumentException) {
        check("{$backend}: an empty queue name is rejected", true);
    }

    try {
        $queue->pop(queues: ['default', 'high', 'default']);
        check("{$backend}: a duplicate queue name is rejected", false);
    } catch (InvalidQueueArgumentException) {
        check("{$backend}: a duplicate queue name is rejected", true);
    }

    check(
        "{$backend}: an empty queue list returns null, not an error",
        $queue->pop(timeoutSeconds: 1, queues: []) === null,
    );

    echo "\n";
}

/**
 * A reservation is finite: a row whose worker died is reclaimed once the
 * visibility timeout passes, with the crashed attempt credited, while a
 * reservation still inside its window stays that worker's and a fresh
 * row's first reservation is unaffected.
 */
function runSqlQueueVisibilityTimeoutChecks(MysqlLink $mysql): void
{
    echo "=== SqlQueue visibility timeout ===\n";

    $mysql->execute('DELETE FROM kinetis_queue_jobs');

    // visibilityTimeoutSeconds=5 (not 2) is a deliberate margin, not an
    // arbitrary number: reserved_at is written and compared via PHP's own
    // time(), whole-second granularity — a worst-case ~1s of truncation
    // slop plus the "not reclaimed yet" check's own up-to-1s poll window
    // meant a 2s timeout left near-zero real margin and flaked under CI
    // jitter. 5s (with sleep(7) below) gives ~3s of margin on both sides,
    // still fast, no longer riding the edge of the clock's own precision.
    $withTimeout = new SqlQueue($mysql, visibilityTimeoutSeconds: 5);
    $withTimeout->push(new IntegrationTestJob('will-be-reclaimed'));
    $first = $withTimeout->pop(timeoutSeconds: 5);
    check('SqlQueue: first pop reports attempts=1', $first?->attempts === 1);
    // Crash: never ack()/release().
    check(
        'SqlQueue: not reclaimed before the visibility timeout elapses',
        $withTimeout->pop(timeoutSeconds: 1, queues: ['default']) === null,
    );
    sleep(7);
    $reclaimed = $withTimeout->pop(timeoutSeconds: 5);
    check('SqlQueue: job is reclaimed after the visibility timeout elapses', $reclaimed !== null);
    check('SqlQueue: reclaimed job reports attempts=2 (the crash plus this attempt)', $reclaimed?->attempts === 2);
    $withTimeout->ack($reclaimed);

    $mysql->execute('DELETE FROM kinetis_queue_jobs');
    $withTimeout->push(new IntegrationTestJob('fresh-row'));
    $fresh = $withTimeout->pop(timeoutSeconds: 5);
    check('SqlQueue: a fresh row still reports attempts=1 on first pop', $fresh?->attempts === 1);
    $withTimeout->ack($fresh);

    echo "\n";
}

/**
 * KINETIS-63: a row that's already been reserved (its reserved_at set
 * inside reserveNext()'s own transaction) but turns out to be malformed
 * once decoded must not strand it forever, or crash the worker. Written
 * directly via a raw INSERT — not through push(), which would never
 * accept malformed data in the first place — the same "bypass the
 * public API to simulate corrupted storage" a hand-edited row or a
 * non-Kinetis writer would produce. Verified against the real database,
 * not mocked: settleIfMalformed()'s own coordination logic is already
 * unit tested (see QueueContractTest), but only a real MySQL round trip
 * can prove the DELETE this backend's settle callback issues genuinely
 * removes the row, rather than merely appearing to under a fake.
 */
function runMalformedRowChecks(MysqlLink $mysql): void
{
    echo "=== SqlQueue: malformed row settlement ===\n";

    $mysql->execute('DELETE FROM kinetis_queue_jobs');

    $mysql->execute(
        'INSERT INTO kinetis_queue_jobs (class, queue, args, available_at, attempts, max_attempts, created_at) '
        . "VALUES (?, 'default', ?, NOW(), 0, NULL, NOW())",
        ['Some\\Job', 'not valid json'],
    );

    $queue = new SqlQueue($mysql);
    $threw = null;

    try {
        $queue->pop(timeoutSeconds: 1);
    } catch (MalformedJobSettledException $e) {
        $threw = $e;
    }

    check('SqlQueue: pop() throws MalformedJobSettledException for a malformed reserved row', $threw !== null);
    check('SqlQueue: the settled exception names the right queue', $threw?->queue === 'default');

    $remaining = $mysql->execute('SELECT COUNT(*) AS c FROM kinetis_queue_jobs')->fetchRow();
    check('SqlQueue: the malformed row was genuinely deleted, not stranded', (int) $remaining['c'] === 0);

    // The loop must genuinely continue: a real, well-formed job pushed
    // right after is still poppable normally.
    $queue->push(new IntegrationTestJob('still works after a malformed row'));
    $recovered = $queue->pop(timeoutSeconds: 5);
    check('SqlQueue: a real job is still popped correctly afterward', $recovered?->args['message'] === 'still works after a malformed row');
    $queue->ack($recovered);

    echo "\n";
}

/**
 * A reclaim gives the row a new reserved_token, so the crashed worker's
 * receipt names a reservation the server no longer holds. Every
 * settlement matches on id plus token, so each of the three affects zero
 * rows and raises StaleJobHandleException, leaving the reclaiming
 * worker's delivery intact and settleable.
 */
function runReservationFencingChecks(MysqlLink $mysql): void
{
    echo "=== SqlQueue: reservation fencing ===\n";

    foreach ([JobSettlement::Ack, JobSettlement::Release, JobSettlement::Fail] as $operation) {
        $mysql->execute('DELETE FROM kinetis_queue_jobs');

        $queue = new SqlQueue($mysql, visibilityTimeoutSeconds: 5);
        $queue->push(new IntegrationTestJob('reclaimed-under-a-slow-worker'));

        $crashed = $queue->pop(timeoutSeconds: 5);
        check("SqlQueue: the first delivery is popped ({$operation->value})", $crashed !== null);
        // Crash: never ack()/release(), and wait out the timeout.
        sleep(7);
        $reclaimed = $queue->pop(timeoutSeconds: 5);
        check("SqlQueue: the row is reclaimed for a second delivery ({$operation->value})", $reclaimed !== null);

        $stale = null;

        try {
            match ($operation) {
                JobSettlement::Ack => $queue->ack($crashed),
                JobSettlement::Release => $queue->release($crashed),
                JobSettlement::Fail => $queue->fail($crashed),
            };
        } catch (StaleJobHandleException $e) {
            $stale = $e;
        }

        check("SqlQueue: a stale {$operation->value}() raises StaleJobHandleException", $stale?->operation === $operation);

        $row = $mysql->execute('SELECT attempts FROM kinetis_queue_jobs')->fetchRow();
        check("SqlQueue: a stale {$operation->value}() wrote nothing", (int) ($row['attempts'] ?? -1) === 1);

        $queue->ack($reclaimed);
        $remaining = $mysql->execute('SELECT COUNT(*) AS c FROM kinetis_queue_jobs')->fetchRow();
        check("SqlQueue: the reclaiming delivery still settles ({$operation->value})", (int) ($remaining['c'] ?? -1) === 0);
    }

    echo "\n";
}

/**
 * MySQL's default collation compares case-insensitively; the queue column
 * is ascii_bin so a named queue is matched byte for byte. Postgres
 * compares that way already.
 */
function runQueueNameCaseChecks(MysqlLink $mysql): void
{
    echo "=== SqlQueue: byte-exact queue names ===\n";

    $mysql->execute('DELETE FROM kinetis_queue_jobs');

    $queue = new SqlQueue($mysql);
    $queue->push(new IntegrationTestJob('lowercase-only'), queue: 'reports');

    check('SqlQueue: a differently-cased queue name pops nothing', $queue->pop(timeoutSeconds: 1, queues: ['Reports']) === null);
    check('SqlQueue: a differently-cased queue name counts nothing', $queue->size('Reports') === 0);
    check('SqlQueue: the exact queue name still counts the job', $queue->size('reports') === 1);

    $popped = $queue->pop(timeoutSeconds: 5, queues: ['reports']);
    check('SqlQueue: the exact queue name pops the job', $popped !== null);
    $queue->ack($popped);

    echo "\n";
}

$mysql = new MysqliAsyncClient(
    getenv('MYSQL_HOST') ?: '127.0.0.1',
    getenv('MYSQL_USER') ?: 'testuser',
    getenv('MYSQL_PASSWORD') ?: 'testpass',
    getenv('MYSQL_DATABASE') ?: 'testdb',
    (int) (getenv('MYSQL_PORT') ?: 3306),
);
$mysql->execute('DROP TABLE IF EXISTS kinetis_queue_jobs');
$mysql->execute(<<<'SQL'
    CREATE TABLE kinetis_queue_jobs (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        class VARCHAR(255) NOT NULL,
        queue VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'default',
        args TEXT NOT NULL,
        available_at TIMESTAMP NOT NULL,
        reserved_at TIMESTAMP NULL,
        reserved_token VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NULL,
        attempts INT UNSIGNED NOT NULL DEFAULT 0,
        max_attempts INT UNSIGNED NULL,
        metadata TEXT NULL,
        created_at TIMESTAMP NOT NULL,
        INDEX kinetis_queue_jobs_queue_available_at_index (queue, available_at, reserved_at)
    )
    SQL);
runQueueChecks('SqlQueue', new SqlQueue($mysql));
runSqlQueueVisibilityTimeoutChecks($mysql);
runReservationFencingChecks($mysql);
runQueueNameCaseChecks($mysql);
runMalformedRowChecks($mysql);

echo "ALL CHECKS PASSED\n";
