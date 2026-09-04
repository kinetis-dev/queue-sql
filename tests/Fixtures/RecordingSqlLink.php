<?php

declare(strict_types=1);

namespace Kinetis\QueueSql\Tests\Fixtures;

use ArrayIterator;
use IteratorAggregate;
use Kinetis\Persistence\Contract\SqlLink;
use Kinetis\Persistence\Contract\SqlResult;
use Kinetis\Persistence\Contract\SqlTransaction;
use RuntimeException;

/**
 * Records every execute() call without touching a real database — the
 * statements it captures are what a test reads its assertions off.
 * query()/beginTransaction() stay unreachable-and-throwing, the same
 * "never touched" idiom SqlQueueTest's own neverTouchedLink() already
 * establishes: no operation exercised against this fake reaches either.
 */
final class RecordingSqlLink implements SqlLink
{
    /** @var list<array{string, array<int|string, mixed>}> */
    public array $executed = [];

    #[\Override]
    public function query(string $sql): SqlResult
    {
        throw new RuntimeException('This link should never be queried.');
    }

    #[\Override]
    public function execute(string $sql, array $params = []): SqlResult
    {
        $this->executed[] = [$sql, $params];

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
        throw new RuntimeException('This link should never begin a transaction.');
    }

    #[\Override]
    public function close(): void
    {
    }

    #[\Override]
    public function isClosed(): bool
    {
        return false;
    }
}
