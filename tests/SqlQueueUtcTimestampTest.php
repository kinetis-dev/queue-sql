<?php

declare(strict_types=1);

namespace Kinetis\QueueSql\Tests;

use Kinetis\Queue\QueuedJob;
use Kinetis\QueueSql\Reservation;
use Kinetis\QueueSql\SqlQueue;
use Kinetis\QueueSql\Tests\Fixtures\RecordingJob;
use Kinetis\QueueSql\Tests\Fixtures\RecordingSqlLink;
use PHPUnit\Framework\TestCase;

/**
 * Every timestamp SqlQueue writes or compares is the worker's instant as
 * UTC wall-clock time. PHP runs here in a zone that is never UTC and
 * observes DST, so a value formatted in PHP's own zone lands hours away
 * from the expected one and fails the bracket.
 */
final class SqlQueueUtcTimestampTest extends TestCase
{
    private string $previousTimezone;

    protected function setUp(): void
    {
        $this->previousTimezone = date_default_timezone_get();
        date_default_timezone_set('America/New_York');
    }

    protected function tearDown(): void
    {
        date_default_timezone_set($this->previousTimezone);
    }

    public function test_a_delayed_push_writes_utc_due_and_creation_times(): void
    {
        $link = new RecordingSqlLink();
        $queue = new SqlQueue($link);

        $before = time();
        $queue->push(new RecordingJob('x'), delaySeconds: 3600);
        $after = time();

        [$sql, $params] = $link->executed[0];

        self::assertStringContainsString('INSERT INTO kinetis_queue_jobs', $sql);
        self::assertUtcBetween($before + 3600, $after + 3600, $params[3], 'available_at');
        self::assertUtcBetween($before, $after, $params[6], 'created_at');
    }

    public function test_a_delayed_release_writes_a_utc_due_time(): void
    {
        $link = new RecordingSqlLink();
        $queue = new SqlQueue($link);
        $job = new QueuedJob(RecordingJob::class, ['message' => 'x'], handle: new Reservation(7, 'abc'), queue: 'default');

        $before = time();
        $queue->release($job, 600);
        $after = time();

        [$sql, $params] = $link->executed[0];

        self::assertStringContainsString('available_at = ?', $sql);
        self::assertUtcBetween($before + 600, $after + 600, $params[0], 'available_at');
    }

    public function test_the_reclaim_cutoff_is_utc(): void
    {
        $link = new RecordingSqlLink();
        $queue = new SqlQueue($link, visibilityTimeoutSeconds: 30);

        $before = time();
        $queue->size('default');
        $after = time();

        [$sql, $params] = $link->executed[0];

        self::assertStringContainsString('reserved_at <= ?', $sql);
        self::assertUtcBetween($before - 30, $after - 30, $params[1], 'reclaim cutoff');
    }

    public function test_a_renewal_restamps_in_utc(): void
    {
        $link = new RecordingSqlLink();
        $queue = new SqlQueue($link);
        $job = new QueuedJob(RecordingJob::class, ['message' => 'x'], handle: new Reservation(7, 'abc'), queue: 'default');

        $before = time();
        $queue->renew($job);
        $after = time();

        self::assertUtcBetween($before, $after, $link->executed[0][1][0], 'reserved_at');
    }

    private static function assertUtcBetween(int $from, int $to, mixed $actual, string $column): void
    {
        self::assertIsString($actual);
        self::assertGreaterThanOrEqual(gmdate('Y-m-d H:i:s', $from), $actual, $column);
        self::assertLessThanOrEqual(gmdate('Y-m-d H:i:s', $to), $actual, $column);
    }
}
