<?php

declare(strict_types=1);

namespace Kinetis\QueueSql\Tests;

use Kinetis\Persistence\Contract\SqlLink;
use Kinetis\Persistence\Contract\SqlResult;
use Kinetis\Persistence\Contract\SqlTransaction;
use Kinetis\QueueSql\SqlQueue;
use InvalidArgumentException;
use LogicException;
use PHPUnit\Framework\TestCase;

/**
 * Constructor validation only — SqlQueue's own backend-specific
 * correctness (reservation, priority cycling, the real concurrent
 * SELECT ... FOR UPDATE SKIP LOCKED race) is deliberately never
 * unit-tested against a fake, matching this package's established "swap
 * the storage, not the whole system, and don't fake what a real backend
 * has to prove" discipline — real-backend verification lives in
 * tests-integration/ instead. This one check is pure PHP validation that
 * throws before $db is ever touched, so a real database has nothing to
 * prove that a fast unit test can't already prove faster.
 */
final class SqlQueueTest extends TestCase
{
    private function neverTouchedLink(): SqlLink
    {
        return new class implements SqlLink {
            public function query(string $sql): SqlResult
            {
                throw new LogicException('This link should never be queried.');
            }

            public function execute(string $sql, array $params = []): SqlResult
            {
                throw new LogicException('This link should never be queried.');
            }

            public function beginTransaction(): SqlTransaction
            {
                throw new LogicException('This link should never be queried.');
            }

            public function close(): void
            {
            }

            public function isClosed(): bool
            {
                return false;
            }
        };
    }

    public function test_a_null_visibility_timeout_is_accepted(): void
    {
        $queue = new SqlQueue($this->neverTouchedLink(), visibilityTimeoutSeconds: null);

        self::assertInstanceOf(SqlQueue::class, $queue);
    }

    public function test_a_positive_visibility_timeout_is_accepted(): void
    {
        $queue = new SqlQueue($this->neverTouchedLink(), visibilityTimeoutSeconds: 30);

        self::assertInstanceOf(SqlQueue::class, $queue);
    }

    public function test_the_minimum_valid_visibility_timeout_of_one_is_accepted(): void
    {
        $queue = new SqlQueue($this->neverTouchedLink(), visibilityTimeoutSeconds: 1);

        self::assertInstanceOf(SqlQueue::class, $queue);
    }

    public function test_a_zero_visibility_timeout_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('visibilityTimeoutSeconds of at least 1 (or null for no timeout), got 0');

        new SqlQueue($this->neverTouchedLink(), visibilityTimeoutSeconds: 0);
    }

    public function test_a_negative_visibility_timeout_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('visibilityTimeoutSeconds of at least 1 (or null for no timeout), got -5');

        new SqlQueue($this->neverTouchedLink(), visibilityTimeoutSeconds: -5);
    }
}
