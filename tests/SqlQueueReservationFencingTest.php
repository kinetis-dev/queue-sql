<?php

declare(strict_types=1);

namespace Kinetis\QueueSql\Tests;

use Kinetis\Queue\Exception\MalformedJobSettledException;
use Kinetis\Queue\Exception\StaleJobHandleException;
use Kinetis\Queue\JobSettlement;
use Kinetis\Queue\QueuedJob;
use Kinetis\QueueSql\Reservation;
use Kinetis\QueueSql\SqlQueue;
use Kinetis\QueueSql\Tests\Fixtures\RecordingJob;
use Kinetis\QueueSql\Tests\Fixtures\ReservableRowSqlLink;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * A visibility timeout hands a reserved row to a second worker while the
 * first may still be running. These checks are about what the first
 * worker's settlement does when that has happened: it names a reservation
 * that is over, so it must write nothing and say so.
 *
 * ReservableRowSqlLink models one row's reservation state and the
 * `id + reserved_token` predicate every settlement carries — enough to
 * drive the reclaim deterministically, with no timing to wait out. The
 * concurrency around it (FOR UPDATE SKIP LOCKED, the timeout comparison
 * itself) is proven against a real server in tests-integration/.
 */
final class SqlQueueReservationFencingTest extends TestCase
{
    private const string TOKEN_PATTERN = '/^[0-9a-f]{32}\z/';

    public function test_a_reservation_hands_back_a_receipt_carrying_the_row_and_a_random_token(): void
    {
        $link = self::link();

        $job = new SqlQueue($link)->pop();

        self::assertInstanceOf(QueuedJob::class, $job);
        self::assertInstanceOf(Reservation::class, $job->handle);
        self::assertSame(7, $job->handle->id);
        self::assertMatchesRegularExpression(self::TOKEN_PATTERN, $job->handle->token);
        self::assertSame($job->handle->token, $link->row['reserved_token']);
    }

    public function test_every_reclaim_of_the_same_row_writes_a_different_token_and_credits_the_crashed_attempt(): void
    {
        $link = self::link();
        $queue = new SqlQueue($link, visibilityTimeoutSeconds: 60);

        $tokens = [];
        $attempts = [];

        for ($reclaim = 0; $reclaim < 3; ++$reclaim) {
            $job = $queue->pop();

            self::assertInstanceOf(Reservation::class, $job?->handle);
            $tokens[] = $job->handle->token;
            $attempts[] = $job->attempts;

            // The previous delivery is never settled — the worker
            // holding it crashed, which is what a timeout reclaims for.
            $link->reservationExpired = true;
        }

        self::assertCount(3, array_unique($tokens));

        foreach ($tokens as $token) {
            self::assertMatchesRegularExpression(self::TOKEN_PATTERN, $token);
        }

        // Successive deliveries of the same row count up: a reclaim
        // credits the delivery it replaces. SqlQueue's class docblock
        // owns why a fresh reservation does not.
        self::assertSame([1, 2, 3], $attempts);
    }

    /**
     * @return list<array{JobSettlement}>
     */
    public static function settlements(): array
    {
        return [[JobSettlement::Ack], [JobSettlement::Release], [JobSettlement::Fail]];
    }

    #[DataProvider('settlements')]
    public function test_a_settlement_from_a_reclaimed_delivery_writes_nothing_and_is_reported_as_stale(JobSettlement $operation): void
    {
        $link = self::link();
        $queue = new SqlQueue($link, visibilityTimeoutSeconds: 60);

        $first = $queue->pop();
        $link->reservationExpired = true;
        $second = $queue->pop();

        self::assertInstanceOf(QueuedJob::class, $first);
        self::assertInstanceOf(QueuedJob::class, $second);

        $stale = null;

        try {
            self::settle($queue, $operation, $first);
        } catch (StaleJobHandleException $e) {
            $stale = $e;
        }

        self::assertNotNull($stale, "{$operation->value}() from the reclaimed delivery must be reported as stale");
        self::assertSame($operation, $stale->operation);
        self::assertStringContainsString('"default" queue', $stale->getMessage());

        // The row is still there, still reserved, and still holds the
        // second worker's token: the stale call wrote nothing at all.
        self::assertNotNull($link->row);
        self::assertInstanceOf(Reservation::class, $second->handle);
        self::assertSame($second->handle->token, $link->row['reserved_token']);
        self::assertSame(1, $link->row['attempts'], 'a stale release() must not credit an attempt against the live delivery');
    }

    #[DataProvider('settlements')]
    public function test_the_reclaiming_delivery_is_still_settleable_after_a_stale_call(JobSettlement $operation): void
    {
        $link = self::link();
        $queue = new SqlQueue($link, visibilityTimeoutSeconds: 60);

        $first = $queue->pop();
        $link->reservationExpired = true;
        $second = $queue->pop();

        self::assertInstanceOf(QueuedJob::class, $first);
        self::assertInstanceOf(QueuedJob::class, $second);

        try {
            $queue->ack($first);
        } catch (StaleJobHandleException) {
            // The subject of the test above; here it is only the state
            // the live delivery has to survive.
        }

        self::settle($queue, $operation, $second);

        if ($operation === JobSettlement::Release) {
            self::assertNotNull($link->row);
            self::assertNull($link->row['reserved_at']);
            self::assertNull($link->row['reserved_token']);
            self::assertSame(2, $link->row['attempts']);

            return;
        }

        self::assertNull($link->row, "{$operation->value}() must have removed the row");
    }

    public function test_a_settlement_whose_row_is_gone_is_reported_as_stale(): void
    {
        $link = self::link();
        $queue = new SqlQueue($link);

        $job = $queue->pop();
        self::assertInstanceOf(QueuedJob::class, $job);

        // Deleted underneath the worker — a clear(), an operator, a
        // settlement already made. Either way the affected-row count is
        // zero and there is nothing to report as settled.
        $link->row = null;

        $this->expectException(StaleJobHandleException::class);
        $queue->ack($job);
    }

    public function test_a_malformed_row_is_settled_through_the_same_fenced_delete(): void
    {
        $link = self::link(['args' => '{not valid json']);
        $queue = new SqlQueue($link);

        $threw = null;

        try {
            $queue->pop();
        } catch (MalformedJobSettledException $e) {
            $threw = $e;
        }

        self::assertNotNull($threw);
        self::assertSame('default', $threw->queue);
        self::assertNull($link->row, 'the reserving delivery still holds the row, so its cleanup removes it');

        [$sql, $params] = $link->executed[array_key_last($link->executed)];

        self::assertStringContainsString('DELETE FROM', $sql);
        self::assertStringContainsString('WHERE id = ? AND reserved_token = ?', $sql);
        self::assertSame(7, $params[0]);
        self::assertMatchesRegularExpression(self::TOKEN_PATTERN, (string) $params[1]);
    }

    /**
     * MySQL compares an `ascii_bin` VARCHAR with padding, so a row stored
     * as `"default "` answers reserveNext()'s `queue IN (...)` for
     * `default` even though the queue-name grammar rejects the value
     * itself. Decoding the stored name is what keeps that row on the
     * malformed path: the caller-facing InvalidQueueArgumentException
     * QueuedJob's constructor would raise is outside what
     * settleIfMalformed() catches, so it would escape pop() and leave the
     * row for a visibility timeout to serve up again.
     */
    public function test_a_stored_queue_name_outside_the_grammar_is_settled_as_malformed(): void
    {
        $link = self::link(['queue' => 'default ']);
        $queue = new SqlQueue($link);

        $threw = null;

        try {
            $queue->pop();
        } catch (MalformedJobSettledException $e) {
            $threw = $e;
        }

        self::assertNotNull($threw);
        self::assertNull($link->row, 'the poison row must be removed rather than left to be popped again');

        [$sql, $params] = $link->executed[array_key_last($link->executed)];

        self::assertStringContainsString('DELETE FROM', $sql);
        self::assertStringContainsString('WHERE id = ? AND reserved_token = ?', $sql);
        self::assertSame(7, $params[0]);
        self::assertMatchesRegularExpression(self::TOKEN_PATTERN, (string) $params[1]);
    }

    /**
     * The cleanup is not exempt from the fence: a reclaim landing between
     * the reservation and the decode failure makes the row somebody
     * else's, and deleting it would destroy a live delivery. The stale
     * exception surfaces from pop() instead, naming the settlement that
     * found nothing.
     */
    public function test_a_malformed_row_reclaimed_before_cleanup_is_not_deleted(): void
    {
        $link = self::link(['args' => '{not valid json']);
        $link->stealReservationOnReserve = true;

        $stale = null;

        try {
            new SqlQueue($link)->pop();
        } catch (StaleJobHandleException $e) {
            $stale = $e;
        }

        self::assertNotNull($stale);
        self::assertSame(JobSettlement::Fail, $stale->operation);
        self::assertNotNull($link->row, 'the row now belongs to the reclaiming delivery and must survive');
    }

    private static function settle(SqlQueue $queue, JobSettlement $operation, QueuedJob $job): void
    {
        match ($operation) {
            JobSettlement::Ack => $queue->ack($job),
            JobSettlement::Release => $queue->release($job),
            JobSettlement::Fail => $queue->fail($job),
        };
    }

    /**
     * @param array<string, mixed> $overrides
     */
    private static function link(array $overrides = []): ReservableRowSqlLink
    {
        return new ReservableRowSqlLink([
            'id' => 7,
            'class' => RecordingJob::class,
            'args' => '{"message":"work"}',
            'queue' => 'default',
            'available_at' => '2026-09-06 12:00:00',
            'reserved_at' => null,
            'reserved_token' => null,
            'attempts' => 0,
            'max_attempts' => null,
            'metadata' => null,
            ...$overrides,
        ]);
    }
}
