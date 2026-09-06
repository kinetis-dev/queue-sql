<?php

declare(strict_types=1);

namespace Kinetis\QueueSql;

use Kinetis\Instrumentation\Telemetry;
use Kinetis\Persistence\Contract\SqlLink;
use Kinetis\Async\Timer;
use Kinetis\Persistence\TransactionGuard;
use Kinetis\Queue\ClearableQueueInterface;
use Kinetis\Queue\Exception\StaleJobHandleException;
use Kinetis\Queue\Job;
use Kinetis\Queue\JobSerializer;
use Kinetis\Queue\JobSettlement;
use Kinetis\Queue\QueueContract;
use Kinetis\Queue\QueuedJob;
use Psr\Log\NullLogger;
use function Kinetis\Async\concurrently;
use Throwable;

/**
 * Typed against the generic Kinetis\Persistence\Contract\SqlLink rather
 * than MysqlLink|PostgresLink: `SELECT ... FOR UPDATE SKIP LOCKED` and the
 * priority-ordering CASE expression below are standard SQL that MySQL 8+
 * and Postgres 9.5+ support identically, so there is no dialect to
 * isolate.
 *
 * Requires the `kinetis_queue_jobs` table — see
 * resources/migrations/create_kinetis_queue_jobs_table.{mysql,pgsql}.php.stub.
 * Two stubs, not one, because auto-incrementing primary key syntax is not
 * portable and migrations are raw SQL by design. The table is application
 * data that may need indexing and tuning, so it belongs in an explicit
 * migration rather than being created as a side effect of runtime.
 *
 * A fresh TransactionGuard is constructed per pop() call. This class lives
 * for the worker's whole lifetime, and TransactionGuard's `$open`
 * bookkeeping array is only cleared by rollbackDangling(), which nothing
 * here calls — every transaction opened here closes in the same method. A
 * throwaway instance has nothing to leak.
 *
 * SQL has no "block until a row appears" primitive, so pop()'s blocking
 * contract is a poll loop suspended through Kinetis\Async\Timer::delay()
 * rather than a real sleep. reserveNext()'s single priority-ordered query
 * already checks every queue atomically, so there is no per-queue sweep to
 * sequence. See QueueInterface for the cross-backend pop() contract.
 *
 * `max_attempts` is set once at push() (SQL NULL meaning "defer to the
 * worker's default") and never changes; it and `attempts` are read back on
 * every pop(), so a caller can choose between release() and fail() without
 * querying the table.
 *
 * $visibilityTimeoutSeconds decides what happens to a row whose worker
 * crashed between pop() and settlement. `null` leaves it reserved forever.
 * A real value makes reserveNext() also match a row whose `reserved_at` is
 * older than `now - $timeout` and increments `attempts` on reclaim, so
 * `maxAttempts` still gives up on a job whose worker keeps crashing.
 * `reserved_at` is written and compared against this process's own
 * `time()`, not the database's clock, so skew between workers shifts when
 * a reservation looks expired.
 *
 * A reclaim makes the earlier delivery obsolete while its worker may
 * still be running, so every reservation and reclaim writes a fresh
 * random `reserved_token` under the row lock and pop() hands it back on
 * QueuedJob::$handle as a Reservation. ack(), release() and fail() match
 * on the row id *and* that token, so the earlier worker's late settlement
 * finds no row and raises Exception\StaleJobHandleException instead of
 * settling the reservation somebody else now holds.
 */
final class SqlQueue implements ClearableQueueInterface
{
    private const TABLE = 'kinetis_queue_jobs';

    private const UPDATE_TABLE = 'UPDATE ' . self::TABLE;

    private const DELETE_TABLE = 'DELETE FROM ' . self::TABLE;

    private const POLL_INTERVAL_SECONDS = 1.0;

    /**
     * @param SqlLink $db
     */
    public function __construct(
        private readonly SqlLink $db,
        private readonly ?int $visibilityTimeoutSeconds = null,
    ) {
        // null means "no timeout" and is the only value with that
        // meaning — 0 or a negative value would make reserveNext()'s own
        // query match a row reserved an instant ago (0) or one reserved
        // in the future relative to now (negative), letting a second
        // worker reclaim an actively-held reservation immediately rather
        // than after it genuinely goes stale.
        if ($visibilityTimeoutSeconds !== null && $visibilityTimeoutSeconds < 1) {
            throw new \InvalidArgumentException(
                "SqlQueue needs a visibilityTimeoutSeconds of at least 1 (or null for no timeout), got {$visibilityTimeoutSeconds}.",
            );
        }
    }

    #[\Override]
    public function push(Job $job, int $delaySeconds = 0, string $queue = 'default', ?int $maxAttempts = null): void
    {
        QueueContract::assertValidPushArguments($delaySeconds, $queue, $maxAttempts);

        $telemetryToken = Telemetry::global()->jobPushStarted($job::class, $queue);

        try {
            $serialized = JobSerializer::serialize($job);
            $now = self::now();
            $metadata = Telemetry::global()->jobPushMetadata($telemetryToken);

            $this->db->execute(
                'INSERT INTO ' . self::TABLE . ' (class, args, queue, available_at, attempts, max_attempts, metadata, created_at) VALUES (?, ?, ?, ?, 0, ?, ?, ?)',
                [
                    $serialized['class'],
                    // PRESERVE_ZERO_FRACTION: without it, an integral-valued
                    // float argument (4.0) encodes as "4" and decodes back
                    // as an int — a silent type change JobSerializer's own
                    // portable-value contract promises never happens.
                    json_encode($serialized['args'], JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION),
                    $queue,
                    self::formatTimestamp(time() + $delaySeconds),
                    $maxAttempts,
                    $metadata === [] ? null : json_encode($metadata, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION),
                    $now,
                ],
            );
            Telemetry::global()->jobPushEnded($telemetryToken, null);
        } catch (Throwable $e) {
            Telemetry::global()->jobPushEnded($telemetryToken, $e);

            throw $e;
        }
    }

    #[\Override]
    public function pop(int $timeoutSeconds = 0, array $queues = ['default']): ?QueuedJob
    {
        QueueContract::assertValidPopArguments($timeoutSeconds, $queues);

        if ($queues === []) {
            return null;
        }

        // Timer::delay() does a raw Fiber::suspend() and needs an
        // existing Fiber, unlike an amphp call. concurrently()'s
        // single-task form gives the poll loop its own Fiber for the
        // duration of this call, so pop() is callable from a plain script
        // as well as from QueueWorker's loop. It is scoped to the loop
        // alone so a job's handle() can call concurrently() itself
        // afterwards; nesting one inside this still-running loop would
        // still hit Revolt's reentrancy error.
        return concurrently([fn (): ?QueuedJob => $this->pollUntilFoundOrTimedOut($timeoutSeconds, $queues)])[0];
    }

    /**
     * @param list<string> $queues
     */
    private function pollUntilFoundOrTimedOut(int $timeoutSeconds, array $queues): ?QueuedJob
    {
        $deadline = $timeoutSeconds > 0 ? microtime(true) + $timeoutSeconds : null;

        while (true) {
            $row = $this->reserveNext($queues);

            if ($row !== null) {
                // reserveNext() wrote this token itself, under the row
                // lock; it is this delivery's half of the receipt rather
                // than stored data the decode step has to validate.
                $reservation = new Reservation($row['id'], (string) $row['reserved_token']);

                return QueueContract::settleIfMalformed(
                    (string) $row['queue'],
                    fn (): QueuedJob => self::rowToQueuedJob($row, $reservation),
                    fn () => $this->settle(JobSettlement::Fail, (string) $row['queue'], $reservation),
                );
            }

            if ($deadline === null) {
                Timer::delay(self::POLL_INTERVAL_SECONDS);

                continue;
            }

            // Bounded by what is left of the deadline rather than always
            // the full interval, so pop() does not overshoot materially.
            $remaining = $deadline - microtime(true);

            if ($remaining <= 0.0) {
                return null;
            }

            Timer::delay(min(self::POLL_INTERVAL_SECONDS, $remaining));
        }
    }

    /**
     * Takes a raw row rather than reading the result set, so it is
     * testable against a hand-built array with no database.
     *
     * Every field goes through a QueueContract decode helper. Many drivers
     * return every column as a string whatever the SQL type, a NULL column
     * reads back as PHP null, and the row could have been written by
     * something other than this class. `args` goes through storedArgs() on
     * top of storedJsonArray(), since a JSON list column would pass a bare
     * is_array() check despite being a shape no push() writes. `attempts`
     * carries a PHP_INT_MAX - 1 ceiling so the `+ 1` below cannot overflow
     * into a float. `max_attempts` is checked for presence first: this
     * class's schema always selects the column, so only a missing key is
     * corruption, which a plain read cannot tell apart from
     * a legitimate SQL NULL. Failures are caught by
     * pollUntilFoundOrTimedOut() through
     * QueueContract::settleIfMalformed(), so a malformed row is settled
     * rather than crashing the worker.
     *
     * $reservation comes from reserveNext(), not from the row: it names
     * the delivery this call is producing, which is what ack()/release()/
     * fail() are later matched against.
     *
     * @param array<string, mixed> $row
     */
    private static function rowToQueuedJob(array $row, Reservation $reservation): QueuedJob
    {
        $class = QueueContract::storedClass($row['class'] ?? null);
        $args = QueueContract::storedArgs(
            QueueContract::storedJsonArray((string) ($row['args'] ?? ''), 'args'),
        );
        $metadata = QueueContract::storedMetadata($row['metadata'] ?? null);

        QueueContract::assertFieldPresent($row, 'max_attempts');
        $maxAttempts = QueueContract::storedNullableInt($row['max_attempts'], 'max_attempts', 0);

        return new QueuedJob(
            $class,
            $args,
            handle: $reservation,
            queue: (string) $row['queue'],
            attempts: QueueContract::storedInt($row['attempts'] ?? null, 'attempts', 0, PHP_INT_MAX - 1) + 1,
            maxAttempts: $maxAttempts,
            metadata: $metadata,
        );
    }

    #[\Override]
    public function ack(QueuedJob $job): void
    {
        $this->settle(JobSettlement::Ack, $job->queue, self::receipt($job));
    }

    /**
     * Clears the reservation and credits the attempt for this delivery
     * only. The token predicate is what keeps a late release() from
     * unreserving a row another worker is actively running and adding an
     * attempt that worker never made.
     */
    #[\Override]
    public function release(QueuedJob $job): void
    {
        $reservation = self::receipt($job);

        self::assertSettled(JobSettlement::Release, $job->queue, $this->db->execute(
            self::UPDATE_TABLE . ' SET reserved_at = NULL, reserved_token = NULL, attempts = attempts + 1'
            . ' WHERE id = ? AND reserved_token = ?',
            [$reservation->id, $reservation->token],
        )->getRowCount());
    }

    #[\Override]
    public function fail(QueuedJob $job): void
    {
        $this->settle(JobSettlement::Fail, $job->queue, self::receipt($job));
    }

    /**
     * Shared by ack()/fail() (a real QueuedJob's own receipt) and the
     * malformed-row settlement path in pollUntilFoundOrTimedOut() (the
     * receipt reserveNext() just wrote for the row a decode failure was
     * caught for) — the same fenced DELETE either way, just reached from
     * two different starting shapes. The malformed path is fenced like
     * any other: a reclaim between reserving the row and failing to
     * decode it makes that row somebody else's, and deleting it would
     * destroy their delivery.
     */
    private function settle(JobSettlement $operation, string $queue, Reservation $reservation): void
    {
        self::assertSettled($operation, $queue, $this->db->execute(
            self::DELETE_TABLE . ' WHERE id = ? AND reserved_token = ?',
            [$reservation->id, $reservation->token],
        )->getRowCount());
    }

    /**
     * A settlement matches on the primary key, so it affects exactly one
     * row or none at all. None means the delivery is over — settled
     * through another call, or reclaimed once its reservation expired —
     * and nothing was written. Anything other than 1, a driver reporting
     * no count included, is that same "not this delivery's row" answer.
     */
    private static function assertSettled(JobSettlement $operation, string $queue, ?int $affected): void
    {
        if ($affected !== 1) {
            throw StaleJobHandleException::forSettlement($operation, $queue);
        }
    }

    /**
     * pop() is the only producer of a handle this backend accepts, so the
     * return type is the check: a QueuedJob from somewhere else fails
     * here rather than reaching a statement with an unusable receipt.
     */
    private static function receipt(QueuedJob $job): Reservation
    {
        /** @var Reservation $reservation */
        $reservation = $job->handle;

        return $reservation;
    }

    /**
     * Unreserved rows on this queue, delayed ones included — a job still
     * inside its push() delay is outstanding work even though no worker
     * can pop it yet. Rows a worker holds (`reserved_at` set) belong to
     * that worker and are excluded — with the same expired-reservation
     * carve-out pop() applies: under a visibility timeout, a reservation
     * older than the timeout is reclaimable, so the job counts as
     * waiting again.
     */
    #[\Override]
    public function size(string $queue = 'default'): int
    {
        [$condition, $params] = $this->waitingCondition($queue);

        $row = $this->db
            ->execute('SELECT COUNT(*) AS c FROM ' . self::TABLE . " WHERE {$condition}", $params)
            ->fetchRow();

        return (int) ($row['c'] ?? 0);
    }

    /**
     * `reserved_at` alone decides what survives, never the visibility
     * timeout size() and pop() read it through: a reservation past that
     * timeout is reclaimable, which says another worker may take the
     * work over, not that nobody is doing it. pop() makes that handover
     * one row at a time under the row lock that keeps it safe; a clear
     * has no handover to make, so it stops at rows no worker claimed.
     */
    #[\Override]
    public function clear(string $queue = 'default'): int
    {
        QueueContract::assertValidQueueName($queue);

        // getRowCount() is nullable on the contract for result sets that
        // cannot report one; a DELETE always can.
        return $this->db
            ->execute(self::DELETE_TABLE . ' WHERE queue = ? AND reserved_at IS NULL', [$queue])
            ->getRowCount() ?? 0;
    }

    /**
     * The "waiting" predicate size() reports on, mirroring
     * reserveNext()'s own reserved-row handling so the two never
     * disagree about which jobs a worker could still pick up — an
     * expired reservation included, since pop() can reclaim it. clear()
     * uses a narrower predicate of its own; see its docblock.
     *
     * @return array{string, list<string>}
     */
    private function waitingCondition(string $queue): array
    {
        QueueContract::assertValidQueueName($queue);

        if ($this->visibilityTimeoutSeconds === null) {
            return ['queue = ? AND reserved_at IS NULL', [$queue]];
        }

        return [
            'queue = ? AND (reserved_at IS NULL OR reserved_at <= ?)',
            [$queue, self::formatTimestamp(time() - $this->visibilityTimeoutSeconds)],
        ];
    }

    /**
     * @param list<string> $queues checked in priority order via a portable
     *     `ORDER BY CASE queue WHEN ? THEN 0 WHEN ? THEN 1 ... END` — not
     *     MySQL's own FIELD(), which Postgres has no equivalent for
     * @return array<string, mixed>|null
     */
    private function reserveNext(array $queues): ?array
    {
        $guard = new TransactionGuard(new NullLogger());

        $inPlaceholders = implode(', ', array_fill(0, count($queues), '?'));
        $casePlaceholders = implode(' ', array_map(
            static fn (int $priority): string => "WHEN ? THEN {$priority}",
            array_keys($queues),
        ));

        $reservedCondition = $this->visibilityTimeoutSeconds !== null
            ? '(reserved_at IS NULL OR reserved_at <= ?)'
            : 'reserved_at IS NULL';

        $sql = 'SELECT * FROM ' . self::TABLE
            . " WHERE queue IN ({$inPlaceholders}) AND available_at <= ? AND {$reservedCondition}"
            . " ORDER BY CASE queue {$casePlaceholders} ELSE " . count($queues) . ' END, id ASC'
            . ' LIMIT 1 FOR UPDATE SKIP LOCKED';

        $params = [...$queues, self::now()];

        if ($this->visibilityTimeoutSeconds !== null) {
            $params[] = self::formatTimestamp(time() - $this->visibilityTimeoutSeconds);
        }

        $params = [...$params, ...$queues];

        /**
         * @psalm-suppress NoValue Psalm's inference for
         *     TransactionGuard::transaction()'s generic T against the
         *     SqlLink/SqlTransaction contracts collapses to an impossible
         *     type for a closure with two return points (null and array).
         *     Not reproducible in a standalone repro with plain generic
         *     types, so the trigger is the real amphp/sql template shapes.
         *     reserveNext()'s behavior is verified against real
         *     MySQL/MariaDB containers in tests-integration/.
         */
        return $guard->transaction($this->db, function ($tx) use ($sql, $params) {
            $row = $tx->execute($sql, $params)->fetchRow();

            if ($row === null) {
                return null;
            }

            // A non-null reserved_at at this point means the row matched
            // via the stale-reclaim half of $reservedCondition, not a
            // fresh reservation — see this class's own docblock for why
            // that gets an attempts increment here and a fresh one doesn't.
            $isStaleReclaim = $row['reserved_at'] !== null;

            // Written while the row lock is held, so the token a
            // settlement is matched against can only be the newest
            // reservation's. random_bytes() rather than a counter or a
            // timestamp: two workers must never derive the same token,
            // and neither may guess another's.
            $token = bin2hex(random_bytes(16));

            if ($isStaleReclaim) {
                $tx->execute(
                    self::UPDATE_TABLE . ' SET reserved_at = ?, reserved_token = ?, attempts = attempts + 1 WHERE id = ?',
                    [self::now(), $token, $row['id']],
                );
                $row['attempts'] = ((int) $row['attempts']) + 1;
            } else {
                $tx->execute(
                    self::UPDATE_TABLE . ' SET reserved_at = ?, reserved_token = ? WHERE id = ?',
                    [self::now(), $token, $row['id']],
                );
            }

            $row['reserved_token'] = $token;

            return $row;
        });
    }

    private static function now(): string
    {
        return self::formatTimestamp(time());
    }

    private static function formatTimestamp(int $unixTimestamp): string
    {
        return date('Y-m-d H:i:s', $unixTimestamp);
    }
}
