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
 * One `kinetis_queue_jobs` row, carrying just enough of the table's
 * behavior to run SqlQueue's reserve and settle statements: a reservation
 * writes the token it is given, and a settlement matches on the row id
 * plus that exact token, byte for byte, reporting how many rows it
 * affected. That predicate is the whole subject of
 * SqlQueueReservationFencingTest; the concurrency the real table enforces
 * around it (FOR UPDATE SKIP LOCKED, the timeout comparison) stays
 * real-backend territory.
 *
 * $reservationExpired stands in for the visibility-timeout cutoff, so a
 * reclaim is a decision the test makes rather than a wait it sits
 * through. $stealReservationOnReserve rewrites the token the instant a
 * reservation is written, which is the one way a fake can put another
 * worker's reclaim between reserving a row and settling it.
 */
final class ReservableRowSqlLink implements SqlLink
{
    /** @var array<string, mixed>|null */
    public ?array $row;

    public bool $reservationExpired = false;

    public bool $stealReservationOnReserve = false;

    /** @var list<array{string, array<int|string, mixed>}> */
    public array $executed = [];

    /**
     * @param array<string, mixed> $row
     */
    public function __construct(array $row)
    {
        $this->row = $row;
    }

    #[\Override]
    public function query(string $sql): SqlResult
    {
        throw new RuntimeException('This link should never be queried without parameters.');
    }

    #[\Override]
    public function execute(string $sql, array $params = []): SqlResult
    {
        $this->executed[] = [$sql, $params];

        if (str_contains($sql, 'FOR UPDATE SKIP LOCKED')) {
            return self::result($this->reservableRow(), null);
        }

        if (str_contains($sql, 'SET reserved_at = ?')) {
            return self::result(null, $this->reserve($sql, $params));
        }

        if (str_contains($sql, 'SET reserved_at = NULL')) {
            return self::result(null, $this->release($sql, $params));
        }

        if (str_starts_with($sql, 'DELETE FROM')) {
            return self::result(null, $this->delete($sql, $params));
        }

        throw new RuntimeException("Unexpected statement: {$sql}");
    }

    #[\Override]
    public function beginTransaction(): SqlTransaction
    {
        return new class ($this) implements SqlTransaction {
            private bool $active = true;

            public function __construct(private readonly ReservableRowSqlLink $link) {}

            public function query(string $sql): SqlResult
            {
                return $this->link->query($sql);
            }

            public function execute(string $sql, array $params = []): SqlResult
            {
                return $this->link->execute($sql, $params);
            }

            public function beginTransaction(): SqlTransaction
            {
                throw new RuntimeException('Nested transactions are not modelled here.');
            }

            public function commit(): void
            {
                $this->active = false;
            }

            public function rollback(): void
            {
                $this->active = false;
            }

            public function isActive(): bool
            {
                return $this->active;
            }

            public function close(): void
            {
                $this->active = false;
            }

            public function isClosed(): bool
            {
                return !$this->active;
            }
        };
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

    /**
     * What reserveNext()'s SELECT matches: an unreserved row, or a
     * reserved one the test has declared past its visibility timeout.
     *
     * @return array<string, mixed>|null
     */
    private function reservableRow(): ?array
    {
        if ($this->row === null) {
            return null;
        }

        return $this->row['reserved_at'] === null || $this->reservationExpired ? $this->row : null;
    }

    /**
     * @param array<int|string, mixed> $params
     */
    private function reserve(string $sql, array $params): int
    {
        if ($this->row === null) {
            return 0;
        }

        $this->row['reserved_at'] = $params[0];
        $this->row['reserved_token'] = $this->stealReservationOnReserve
            ? 'ffffffffffffffffffffffffffffffff'
            : $params[1];

        if (str_contains($sql, 'attempts = attempts + 1')) {
            $this->row['attempts'] = ((int) $this->row['attempts']) + 1;
        }

        return 1;
    }

    /**
     * @param array<int|string, mixed> $params
     */
    private function release(string $sql, array $params): int
    {
        if (!$this->matches($sql, $params)) {
            return 0;
        }

        /** @var array<string, mixed> $row */
        $row = $this->row;
        $row['reserved_at'] = null;
        $row['reserved_token'] = null;
        $row['attempts'] = ((int) $row['attempts']) + 1;
        $this->row = $row;

        return 1;
    }

    /**
     * @param array<int|string, mixed> $params
     */
    private function delete(string $sql, array $params): int
    {
        if (!$this->matches($sql, $params)) {
            return 0;
        }

        $this->row = null;

        return 1;
    }

    /**
     * The predicate is read off the statement rather than assumed, so a
     * settlement that stopped naming the token matches on the row id
     * alone here, exactly as a real server would answer it. Both values
     * are compared byte for byte — what the ascii_bin columns in the
     * shipped MySQL stub buy against MySQL's own default collation.
     *
     * @param array<int|string, mixed> $params
     */
    private function matches(string $sql, array $params): bool
    {
        if ($this->row === null) {
            return false;
        }

        if (!str_contains($sql, 'WHERE id = ? AND reserved_token = ?')) {
            return $this->row['id'] === $params[0];
        }

        return $this->row['id'] === $params[0] && $this->row['reserved_token'] === $params[1];
    }

    /**
     * @param array<string, mixed>|null $row
     */
    private static function result(?array $row, ?int $affected): SqlResult
    {
        return new class ($row, $affected) implements SqlResult, IteratorAggregate {
            /**
             * @param array<string, mixed>|null $row
             */
            public function __construct(
                private ?array $row,
                private readonly ?int $affected,
            ) {}

            public function fetchRow(): ?array
            {
                $row = $this->row;
                $this->row = null;

                return $row;
            }

            public function getRowCount(): ?int
            {
                return $this->affected;
            }

            public function getColumnCount(): ?int
            {
                return null;
            }

            public function getLastInsertId(): ?int
            {
                return null;
            }

            public function getIterator(): ArrayIterator
            {
                return new ArrayIterator($this->row === null ? [] : [$this->row]);
            }
        };
    }
}
