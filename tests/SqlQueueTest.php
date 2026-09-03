<?php

declare(strict_types=1);

namespace Kinetis\QueueSql\Tests;

use Kinetis\Persistence\Contract\SqlLink;
use Kinetis\Persistence\Contract\SqlResult;
use Kinetis\Persistence\Contract\SqlTransaction;
use Kinetis\Queue\Exception\InvalidDelaySecondsException;
use Kinetis\Queue\Exception\InvalidMaxAttemptsException;
use Kinetis\Queue\Exception\InvalidPopTimeoutException;
use Kinetis\Queue\Exception\InvalidQueueNameException;
use Kinetis\Queue\Exception\MalformedQueuedJobDataException;
use Kinetis\QueueSql\SqlQueue;
use Kinetis\QueueSql\Tests\Fixtures\RecordingJob;
use InvalidArgumentException;
use LogicException;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;

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

    public function test_push_rejects_an_empty_queue_name_before_ever_touching_the_database(): void
    {
        $queue = new SqlQueue($this->neverTouchedLink());

        $this->expectException(InvalidQueueNameException::class);
        $queue->push(new RecordingJob('should never be persisted'), queue: '');
    }

    public function test_pop_rejects_a_negative_timeout_before_ever_touching_the_database(): void
    {
        $queue = new SqlQueue($this->neverTouchedLink());

        $this->expectException(InvalidPopTimeoutException::class);
        $queue->pop(-1);
    }

    public function test_pop_rejects_a_duplicate_queue_name_before_ever_touching_the_database(): void
    {
        $queue = new SqlQueue($this->neverTouchedLink());

        $this->expectException(InvalidQueueNameException::class);
        $queue->pop(0, ['default', 'default']);
    }

    public function test_pop_returns_null_immediately_for_an_empty_queue_list_before_ever_touching_the_database(): void
    {
        $queue = new SqlQueue($this->neverTouchedLink());

        self::assertNull($queue->pop(5, []));
    }

    public function test_size_rejects_an_empty_queue_name_before_ever_touching_the_database(): void
    {
        $queue = new SqlQueue($this->neverTouchedLink());

        $this->expectException(InvalidQueueNameException::class);
        $queue->size('');
    }

    public function test_size_rejects_a_malformed_queue_name_before_ever_touching_the_database(): void
    {
        $queue = new SqlQueue($this->neverTouchedLink());

        $this->expectException(InvalidQueueNameException::class);
        $queue->size('has spaces');
    }

    public function test_clear_rejects_an_empty_queue_name_before_ever_touching_the_database(): void
    {
        $queue = new SqlQueue($this->neverTouchedLink());

        $this->expectException(InvalidQueueNameException::class);
        $queue->clear('');
    }

    public function test_push_rejects_a_negative_delay_before_ever_touching_the_database(): void
    {
        $queue = new SqlQueue($this->neverTouchedLink());

        $this->expectException(InvalidDelaySecondsException::class);
        $queue->push(new RecordingJob('should never be persisted'), delaySeconds: -1);
    }

    public function test_push_rejects_a_negative_max_attempts_before_ever_touching_the_database(): void
    {
        $queue = new SqlQueue($this->neverTouchedLink());

        $this->expectException(InvalidMaxAttemptsException::class);
        $queue->push(new RecordingJob('should never be persisted'), maxAttempts: -1);
    }

    /**
     * rowToQueuedJob() is where a corrupted row's malformed attempts/
     * max_attempts value is actually caught — proven directly with a
     * hand-built row array (no real database needed, since this method
     * was extracted specifically to make that possible), so the wiring
     * between it and QueueContract::coerceStoredInteger() is exercised
     * too, not just coerceStoredInteger()'s own unit-level behavior.
     */
    public function test_row_to_queued_job_rejects_a_non_numeric_stored_attempts_value(): void
    {
        $rowToQueuedJob = new ReflectionMethod(SqlQueue::class, 'rowToQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"attempts"');
        $rowToQueuedJob->invoke(null, [
            'id' => 1,
            'class' => RecordingJob::class,
            'args' => '{"message":"x"}',
            'queue' => 'default',
            'attempts' => 'garbage',
            'max_attempts' => null,
            'metadata' => null,
        ]);
    }

    public function test_row_to_queued_job_rejects_a_non_numeric_stored_max_attempts_value(): void
    {
        $rowToQueuedJob = new ReflectionMethod(SqlQueue::class, 'rowToQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"max_attempts"');
        $rowToQueuedJob->invoke(null, [
            'id' => 1,
            'class' => RecordingJob::class,
            'args' => '{"message":"x"}',
            'queue' => 'default',
            'attempts' => 0,
            'max_attempts' => 'garbage',
            'metadata' => null,
        ]);
    }

    /**
     * The reviewer's own reported overflow gap, at the real decode level:
     * a stored completed-attempts count of exactly PHP_INT_MAX is
     * syntactically a perfectly valid integer — coerceStoredInteger()
     * alone would accept it — but rowToQueuedJob()'s own `+ 1` would
     * silently overflow it to a float, which would then fail QueuedJob's
     * strictly-typed constructor with a confusing TypeError. This proves
     * the real, wired decode path rejects it cleanly instead, via
     * QueueContract::coerceStoredCompletedAttempts() — as the string many
     * real database drivers actually return a column value as, not the
     * native int form.
     */
    public function test_row_to_queued_job_rejects_a_stored_attempts_value_of_php_int_max(): void
    {
        $rowToQueuedJob = new ReflectionMethod(SqlQueue::class, 'rowToQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('PHP_INT_MAX');
        $rowToQueuedJob->invoke(null, [
            'id' => 1,
            'class' => RecordingJob::class,
            'args' => '{"message":"x"}',
            'queue' => 'default',
            'attempts' => (string) PHP_INT_MAX,
            'max_attempts' => null,
            'metadata' => null,
        ]);
    }

    public function test_row_to_queued_job_rejects_an_args_column_that_is_not_valid_json(): void
    {
        $rowToQueuedJob = new ReflectionMethod(SqlQueue::class, 'rowToQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"args"');
        $rowToQueuedJob->invoke(null, [
            'id' => 1,
            'class' => RecordingJob::class,
            'args' => '{not valid json',
            'queue' => 'default',
            'attempts' => 0,
            'max_attempts' => null,
            'metadata' => null,
        ]);
    }

    public function test_row_to_queued_job_rejects_an_args_column_that_is_not_a_json_object(): void
    {
        $rowToQueuedJob = new ReflectionMethod(SqlQueue::class, 'rowToQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"args"');
        $rowToQueuedJob->invoke(null, [
            'id' => 1,
            'class' => RecordingJob::class,
            'args' => '"just a string"',
            'queue' => 'default',
            'attempts' => 0,
            'max_attempts' => null,
            'metadata' => null,
        ]);
    }

    /**
     * A JSON *list* args column value ('["value"]', no object keys) is a
     * real, distinct malformed shape from "not an object at all" above —
     * coerceStoredJsonArray()'s own is_array() check would have accepted
     * it. Confirming it throws MalformedQueuedJobDataException here, from
     * rowToQueuedJob() itself (the exact function
     * pollUntilFoundOrTimedOut() wraps in
     * QueueContract::settleIfMalformed()), is what proves this reaches
     * the settle-and-remove path rather than QueueWorker's ordinary
     * job-execution failure handling.
     */
    public function test_row_to_queued_job_rejects_an_args_column_that_is_a_json_list(): void
    {
        $rowToQueuedJob = new ReflectionMethod(SqlQueue::class, 'rowToQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"args"');
        $rowToQueuedJob->invoke(null, [
            'id' => 1,
            'class' => RecordingJob::class,
            'args' => '["positional value"]',
            'queue' => 'default',
            'attempts' => 0,
            'max_attempts' => null,
            'metadata' => null,
        ]);
    }

    public function test_row_to_queued_job_rejects_a_missing_or_null_class_column(): void
    {
        $rowToQueuedJob = new ReflectionMethod(SqlQueue::class, 'rowToQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"class"');
        $rowToQueuedJob->invoke(null, [
            'id' => 1,
            'class' => null,
            'args' => '{"message":"x"}',
            'queue' => 'default',
            'attempts' => 0,
            'max_attempts' => null,
            'metadata' => null,
        ]);
    }

    public function test_row_to_queued_job_rejects_a_metadata_column_with_a_non_string_key(): void
    {
        $rowToQueuedJob = new ReflectionMethod(SqlQueue::class, 'rowToQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"metadata"');
        $rowToQueuedJob->invoke(null, [
            'id' => 1,
            'class' => RecordingJob::class,
            'args' => '{"message":"x"}',
            'queue' => 'default',
            'attempts' => 0,
            'max_attempts' => null,
            'metadata' => '["not","a","map"]',
        ]);
    }

    /**
     * A prior version of this method silently mapped a present but
     * non-string metadata column value to null ("no metadata"), rather
     * than rejecting it — a real, corrupted integer/array/object column
     * value was accepted as though metadata had never been stored at
     * all, contrary to the shared string-to-string map contract every
     * other backend's own metadata field is held to. This proves every
     * present value, not only string-typed ones, now reaches the
     * coercer.
     */
    public function test_row_to_queued_job_rejects_a_present_non_string_metadata_column(): void
    {
        $rowToQueuedJob = new ReflectionMethod(SqlQueue::class, 'rowToQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"metadata"');
        $rowToQueuedJob->invoke(null, [
            'id' => 1,
            'class' => RecordingJob::class,
            'args' => '{"message":"x"}',
            'queue' => 'default',
            'attempts' => 0,
            'max_attempts' => null,
            'metadata' => 12345,
        ]);
    }

    /**
     * This class's own fixed table schema always selects the
     * `max_attempts` column, so its outright absence from the row array
     * — not merely a present SQL NULL, which legitimately means "no
     * override" — is a sign of a hand-built row or a genuine schema
     * drift, either way corrupted storage this backend has never seen
     * before.
     */
    public function test_row_to_queued_job_rejects_a_row_missing_the_max_attempts_key_entirely(): void
    {
        $rowToQueuedJob = new ReflectionMethod(SqlQueue::class, 'rowToQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"max_attempts"');
        $this->expectExceptionMessage('missing entirely');
        $rowToQueuedJob->invoke(null, [
            'id' => 1,
            'class' => RecordingJob::class,
            'args' => '{"message":"x"}',
            'queue' => 'default',
            'attempts' => 0,
            'metadata' => null,
        ]);
    }
}
