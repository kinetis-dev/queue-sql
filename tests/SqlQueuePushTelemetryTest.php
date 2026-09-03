<?php

declare(strict_types=1);

namespace Kinetis\QueueSql\Tests;

use Kinetis\Instrumentation\NullTelemetry;
use Kinetis\Instrumentation\Telemetry;
use Kinetis\Queue\Exception\InvalidDelaySecondsException;
use Kinetis\QueueSql\SqlQueue;
use Kinetis\QueueSql\Tests\Fixtures\RecordingJob;
use Kinetis\QueueSql\Tests\Fixtures\RecordingSqlLink;
use Kinetis\QueueSql\Tests\Fixtures\ThrowingTelemetry;
use PHPUnit\Framework\TestCase;

/**
 * A real durable-adapter regression, deliberately distinct from
 * SqlQueueTest's own constructor-validation-only scope: not proving
 * SqlQueue's SQL generation or query correctness (that stays
 * real-backend-only, per SqlQueueTest's own docblock and this package's
 * established "swap the storage, not the whole system" discipline), only
 * that push()'s control flow around telemetry is safe — the actual
 * INSERT happens exactly once and push() returns normally even when the
 * jobPushEnded() telemetry call that follows a successful send fails.
 * kinetis/queue's own SyncQueue test proves the identical containment
 * for an in-memory, non-durable producer; this proves it for a producer
 * whose own send is a real, external side effect a duplicate would
 * actually duplicate.
 */
final class SqlQueuePushTelemetryTest extends TestCase
{
    /**
     * Telemetry::global() is a real per-process singleton — restore a
     * clean one afterward, or a later, unrelated test would silently
     * observe it.
     */
    protected function tearDown(): void
    {
        Telemetry::global()->swap(new NullTelemetry());
    }

    public function test_a_successful_send_is_not_duplicated_or_reported_as_failed_when_telemetry_fails_to_end(): void
    {
        $telemetry = new ThrowingTelemetry();
        $telemetry->throwOnJobPushEnded = true;
        Telemetry::global()->swap($telemetry);

        $link = new RecordingSqlLink();
        $queue = new SqlQueue($link);

        // push() itself must not throw — the send succeeded, and the
        // ending telemetry hook's own failure is Telemetry's to contain,
        // never push()'s to report.
        $queue->push(new RecordingJob('hello'), queue: 'default', maxAttempts: 3);

        self::assertCount(1, $link->executed, 'the INSERT ran exactly once — not duplicated, not skipped');
        self::assertStringContainsString('INSERT INTO', $link->executed[0][0]);
    }

    /**
     * QueueContract::assertValidPushArguments() must run before
     * Telemetry::global()->jobPushStarted() is ever called — proven
     * directly: a telemetry backend whose jobPushStarted() throws
     * unconditionally would surface *its own* RuntimeException instead of
     * the real validation failure if the ordering were ever reversed, and
     * the INSERT below would never run at all either way.
     */
    public function test_a_negative_delay_is_rejected_before_telemetry_is_ever_started(): void
    {
        $telemetry = new ThrowingTelemetry();
        $telemetry->throwOnJobPushStarted = true;
        Telemetry::global()->swap($telemetry);

        $link = new RecordingSqlLink();
        $queue = new SqlQueue($link);

        try {
            $queue->push(new RecordingJob('should never be persisted'), delaySeconds: -1);
            self::fail('Expected the negative delay to be rejected.');
        } catch (InvalidDelaySecondsException) {
            // expected
        }

        self::assertSame([], $link->executed, 'no INSERT must have run for a rejected push()');
    }
}
