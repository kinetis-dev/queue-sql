<?php

declare(strict_types=1);

namespace Kinetis\QueueSql\Tests\Fixtures;

use ArrayIterator;
use IteratorAggregate;
use Kinetis\Persistence\Contract\SqlResult;
use Kinetis\Persistence\Contract\SqlTransaction;
use RuntimeException;
use Throwable;

/**
 * An open transaction a caller owns, recording what SqlQueue::pushOn()
 * runs on it and counting the lifecycle calls pushOn() must never make.
 * commit()/rollback()/close() count rather than throw, so a test names
 * what it proves instead of reading an exception out of the wrong layer.
 *
 * $failExecuteWith makes the enqueue statement fail the way a real
 * transaction's does, which is how the telemetry-on-failure path is
 * reachable without a database.
 */
final class RecordingSqlTransaction implements SqlTransaction
{
    /** @var list<array{string, array<int|string, mixed>}> */
    public array $executed = [];

    public int $commits = 0;

    public int $rollbacks = 0;

    public int $closes = 0;

    public ?Throwable $failExecuteWith = null;

    #[\Override]
    public function query(string $sql): SqlResult
    {
        throw new RuntimeException('This transaction should never be queried without parameters.');
    }

    #[\Override]
    public function execute(string $sql, array $params = []): SqlResult
    {
        $this->executed[] = [$sql, $params];

        if ($this->failExecuteWith !== null) {
            throw $this->failExecuteWith;
        }

        return new class implements SqlResult, IteratorAggregate {
            #[\Override]
            public function fetchRow(): ?array
            {
                return null;
            }

            #[\Override]
            public function getRowCount(): ?int
            {
                return 1;
            }

            #[\Override]
            public function getColumnCount(): ?int
            {
                return null;
            }

            #[\Override]
            public function getLastInsertId(): ?int
            {
                return 1;
            }

            #[\Override]
            public function getIterator(): ArrayIterator
            {
                return new ArrayIterator([]);
            }
        };
    }

    #[\Override]
    public function beginTransaction(): SqlTransaction
    {
        throw new RuntimeException('This transaction should never be nested.');
    }

    #[\Override]
    public function commit(): void
    {
        ++$this->commits;
    }

    #[\Override]
    public function rollback(): void
    {
        ++$this->rollbacks;
    }

    #[\Override]
    public function isActive(): bool
    {
        return true;
    }

    #[\Override]
    public function close(): void
    {
        ++$this->closes;
    }

    #[\Override]
    public function isClosed(): bool
    {
        return false;
    }
}
