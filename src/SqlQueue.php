<?php

declare(strict_types=1);

namespace Kinetis\QueueSql;

use Kinetis\Instrumentation\Telemetry;
use Kinetis\Persistence\Contract\SqlLink;
use Kinetis\Async\Timer;
use Kinetis\Persistence\TransactionGuard;
use Kinetis\Queue\Job;
use Kinetis\Queue\JobSerializer;
use Kinetis\Queue\QueuedJob;
use Kinetis\Queue\QueueInterface;
use Psr\Log\NullLogger;
use function Kinetis\Async\concurrently;
use Throwable;

/**
 * Typed against the generic Kinetis\Persistence\Contract\SqlLink, not
 * MysqlLink|PostgresLink —
 * the same reasoning SqlMigrationRepository already uses: `SELECT ...
 * FOR UPDATE SKIP LOCKED` is standard SQL supported identically by both
 * MySQL 8+ and Postgres 9.5+, so there's no dialect to isolate — including
 * the priority-ordering CASE expression below, which is standard SQL too
 * (not MySQL's own FIELD(), which Postgres has no equivalent for).
 *
 * Requires the `kinetis_queue_jobs` table — see
 * resources/migrations/create_kinetis_queue_jobs_table.{mysql,pgsql}.php.stub,
 * ready-to-copy Kinetis\Migrations migration files (two, not one: the
 * auto-incrementing primary key syntax itself isn't portable between
 * MySQL and Postgres, so — matching migrations being raw SQL by design,
 * with no DDL abstraction — there's no single shared stub), not
 * auto-created by this class the way SqlMigrationRepository auto-creates
 * its own tiny bookkeeping table. That table is a fixed shape that will
 * basically never change; a queue jobs table is real application data —
 * indexed, may need tuning — better managed as an explicit, versioned
 * migration than created implicitly as a side effect of normal runtime
 * operation.
 *
 * A fresh TransactionGuard is constructed per pop() call rather than
 * injected and reused — this class is registered once on AppScope and
 * lives for the worker's entire lifetime, but TransactionGuard's own
 * `$open` bookkeeping array is only ever cleared by rollbackDangling(),
 * which nothing here ever calls (every transaction this class opens
 * always closes within the same method call). Reusing one shared
 * instance across a worker processing millions of jobs over its lifetime
 * would grow that array forever; a throwaway instance per call has
 * nothing to leak.
 *
 * No native "block until a row appears" primitive exists in SQL, unlike
 * Redis's BLPOP, so pop()'s blocking contract is implemented as a poll
 * loop instead, suspended between attempts via Kinetis\Async\Timer::delay()
 * rather than a real sleep() — the caller can't tell polling is
 * happening underneath.
 *
 * `max_attempts` is set once at push() from that call's own $maxAttempts
 * argument (SQL NULL meaning "defer to the worker's own default" — see
 * QueueWorker) and never changes afterward; both it and `attempts` are
 * read back on every pop() into QueuedJob, so a caller can decide between
 * release() and fail() without querying the table directly.
 *
 * $visibilityTimeoutSeconds closes a real gap: without it, a worker
 * that pops a job and then crashes
 * before ack()/release() runs leaves that row permanently stranded, with
 * `reserved_at` set forever and no other worker able to reclaim it —
 * `null` (the default) preserves that exact pre-existing behavior
 * unchanged. Passing a real value makes reserveNext()'s own query also
 * match a row whose `reserved_at` is older than `now - $timeout`,
 * treating it as available again — the standard "visibility timeout"
 * pattern SQS's own ReceiveMessage/VisibilityTimeout already uses.
 * Reclaiming a stale row increments `attempts` the same way an explicit
 * release() call already does (not merely re-reading the same value on
 * every crash-loop iteration), so `maxAttempts` still eventually gives up
 * on a job whose worker keeps crashing rather than retrying it forever
 * with no cap; a genuinely fresh (never-reserved) row's own first
 * reservation is unaffected, leaving `attempts` untouched until an
 * actual release() call.
 */
final class SqlQueue implements QueueInterface
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
        $telemetryToken = Telemetry::global()->jobPushStarted($job::class, $queue);

        try {
            $serialized = JobSerializer::serialize($job);
            $now = self::now();
            $metadata = Telemetry::global()->jobPushMetadata($telemetryToken);

            $this->db->execute(
                'INSERT INTO ' . self::TABLE . ' (class, args, queue, available_at, attempts, max_attempts, metadata, created_at) VALUES (?, ?, ?, ?, 0, ?, ?, ?)',
                [
                    $serialized['class'],
                    json_encode($serialized['args'], JSON_THROW_ON_ERROR),
                    $queue,
                    self::formatTimestamp(time() + $delaySeconds),
                    $maxAttempts,
                    $metadata === [] ? null : json_encode($metadata, JSON_THROW_ON_ERROR),
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
        if ($queues === []) {
            return null;
        }

        // The poll loop below suspends between attempts via Timer::delay(),
        // which does a raw Fiber::suspend() — it requires an existing Fiber
        // to suspend, unlike an amphp call (TransactionGuard's own queries
        // included), which tolerates being awaited from plain top-level
        // code. concurrently() (its single-task form here) gives the loop
        // its own short-lived Fiber for exactly the duration of one pop()
        // call, so pop() is safely callable from anywhere — a plain script,
        // QueueWorker's loop — without the caller needing to know or care
        // that a Fiber is involved underneath. Scoped tightly around just
        // the loop, not the caller's surrounding job-invocation code, so a
        // job's own handle() is free to call concurrently() itself
        // afterwards without hitting Revolt's "event loop is already
        // running" reentrancy error — nesting a second concurrently() call
        // *inside* this one's still-running loop would still hit that
        // error, the same limitation this class's own FOR UPDATE SKIP
        // LOCKED concurrency has.
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
                /** @var array<string, mixed> $args */
                $args = json_decode((string) $row['args'], true, flags: JSON_THROW_ON_ERROR);
                /** @var class-string<Job> $class */
                $class = $row['class'];

                /** @var array<string, string> $metadata */
                $metadata = isset($row['metadata']) && \is_string($row['metadata'])
                    ? json_decode($row['metadata'], true, flags: JSON_THROW_ON_ERROR)
                    : [];

                return new QueuedJob(
                    $class,
                    $args,
                    handle: $row['id'],
                    queue: (string) $row['queue'],
                    attempts: (int) $row['attempts'] + 1,
                    maxAttempts: $row['max_attempts'] !== null ? (int) $row['max_attempts'] : null,
                    metadata: $metadata,
                );
            }

            if ($deadline !== null && microtime(true) >= $deadline) {
                return null;
            }

            Timer::delay(self::POLL_INTERVAL_SECONDS);
        }
    }

    #[\Override]
    public function ack(QueuedJob $job): void
    {
        $this->db->execute(self::DELETE_TABLE . ' WHERE id = ?', [$job->handle]);
    }

    #[\Override]
    public function release(QueuedJob $job): void
    {
        $this->db->execute(
            self::UPDATE_TABLE . ' SET reserved_at = NULL, attempts = attempts + 1 WHERE id = ?',
            [$job->handle],
        );
    }

    #[\Override]
    public function fail(QueuedJob $job): void
    {
        $this->db->execute(self::DELETE_TABLE . ' WHERE id = ?', [$job->handle]);
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

    #[\Override]
    public function clear(string $queue = 'default'): int
    {
        [$condition, $params] = $this->waitingCondition($queue);

        // getRowCount() is nullable on the contract for result sets that
        // cannot report one; a DELETE always can.
        return $this->db
            ->execute(self::DELETE_TABLE . " WHERE {$condition}", $params)
            ->getRowCount() ?? 0;
    }

    /**
     * The "waiting" predicate size() and clear() share, mirroring
     * reserveNext()'s own reserved-row handling so the three never
     * disagree about which jobs a worker could still pick up.
     *
     * @return array{string, list<string>}
     */
    private function waitingCondition(string $queue): array
    {
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
         * @psalm-suppress NoValue Psalm's template inference for
         *     TransactionGuard::transaction()'s generic T, combined with
         *     the SqlLink/SqlTransaction contracts,
         *     collapses to an impossible type here for a closure with two
         *     return points (null and array) — confirmed not reproducible
         *     in a minimal, simplified standalone repro using plain,
         *     non-Amp generic types, so the trigger is specific to the
         *     real amphp/sql template shapes, not this method's own logic.
         *     reserveNext()'s actual behavior (returning the row or null)
         *     is independently verified against real MySQL/MariaDB
         *     containers — see tests-integration/ in this package.
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

            if ($isStaleReclaim) {
                $tx->execute(
                    self::UPDATE_TABLE . ' SET reserved_at = ?, attempts = attempts + 1 WHERE id = ?',
                    [self::now(), $row['id']],
                );
                $row['attempts'] = ((int) $row['attempts']) + 1;
            } else {
                $tx->execute(self::UPDATE_TABLE . ' SET reserved_at = ? WHERE id = ?', [self::now(), $row['id']]);
            }

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
