<?php

declare(strict_types=1);

/**
 * Real-backend regression coverage for SqlQueue — it has no committed
 * PHPUnit test beyond constructor validation, by design: a mocked "was
 * this method called with X" test can't prove backend-specific
 * correctness (FOR UPDATE SKIP LOCKED, priority-queue cycling, the
 * visibility-timeout reclaim). This runs the same checks originally
 * verified by hand, on every CI push instead of once.
 */

require __DIR__ . '/../vendor/autoload.php';

use Kinetis\Persistence\Contract\MysqlLink;
use Kinetis\Persistence\Driver\MysqliAsyncClient;
use Kinetis\Queue\Exception\InvalidPopTimeoutException;
use Kinetis\Queue\Exception\InvalidQueueNameException;
use Kinetis\Queue\Exception\MalformedJobSettledException;
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
    } catch (InvalidPopTimeoutException) {
        check("{$backend}: a negative timeout is rejected", true);
    }

    try {
        $queue->pop(queues: ['default', '']);
        check("{$backend}: an empty queue name is rejected", false);
    } catch (InvalidQueueNameException) {
        check("{$backend}: an empty queue name is rejected", true);
    }

    try {
        $queue->pop(queues: ['default', 'high', 'default']);
        check("{$backend}: a duplicate queue name is rejected", false);
    } catch (InvalidQueueNameException) {
        check("{$backend}: a duplicate queue name is rejected", true);
    }

    check(
        "{$backend}: an empty queue list returns null, not an error",
        $queue->pop(timeoutSeconds: 1, queues: []) === null,
    );

    echo "\n";
}

/**
 * §6.9 of the independent evaluation report: a crashed worker's reserved
 * row stayed stranded forever on SqlQueue, undisclosed. $visibilityTimeoutSeconds
 * closes it — this proves both halves: no timeout configured behaves
 * exactly as before (stranded forever), a real timeout reclaims the row
 * and correctly increments attempts (crediting the crashed attempt),
 * while a genuinely fresh row's first reservation stays unaffected.
 */
function runSqlQueueVisibilityTimeoutChecks(MysqlLink $mysql): void
{
    echo "=== SqlQueue visibility timeout ===\n";

    $mysql->execute('DELETE FROM kinetis_queue_jobs');

    $withoutTimeout = new SqlQueue($mysql);
    $withoutTimeout->push(new IntegrationTestJob('stranded-forever'));
    $popped = $withoutTimeout->pop(timeoutSeconds: 5);
    check('SqlQueue: job popped once', $popped !== null);
    // Deliberately never ack()/release() — simulating a crashed worker.
    sleep(2);
    check(
        'SqlQueue: without a visibility timeout, a crashed worker\'s job is never reclaimed',
        $withoutTimeout->pop(timeoutSeconds: 1, queues: ['default']) === null,
    );

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
        queue VARCHAR(255) NOT NULL DEFAULT 'default',
        args TEXT NOT NULL,
        available_at TIMESTAMP NOT NULL,
        reserved_at TIMESTAMP NULL,
        attempts INT UNSIGNED NOT NULL DEFAULT 0,
        max_attempts INT UNSIGNED NULL,
        metadata TEXT NULL,
        created_at TIMESTAMP NOT NULL,
        INDEX kinetis_queue_jobs_queue_available_at_index (queue, available_at, reserved_at)
    )
    SQL);
runQueueChecks('SqlQueue', new SqlQueue($mysql));
runSqlQueueVisibilityTimeoutChecks($mysql);
runMalformedRowChecks($mysql);

echo "ALL CHECKS PASSED\n";
