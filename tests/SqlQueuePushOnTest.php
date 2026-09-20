<?php

declare(strict_types=1);

namespace Kinetis\QueueSql\Tests;

use Kinetis\Instrumentation\NullTelemetry;
use Kinetis\Instrumentation\Telemetry;
use Kinetis\Queue\Exception\InvalidQueueArgumentException;
use Kinetis\QueueSql\SqlQueue;
use Kinetis\QueueSql\Tests\Fixtures\RecordingJob;
use Kinetis\QueueSql\Tests\Fixtures\RecordingSqlLink;
use Kinetis\QueueSql\Tests\Fixtures\RecordingSqlTransaction;
use Kinetis\QueueSql\Tests\Fixtures\ThrowingTelemetry;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Throwable;

/**
 * pushOn() enqueues on a transaction the caller owns. What that has to
 * answer for without a database: the statement goes to the supplied
 * transaction and nowhere else, the envelope and the validation are
 * push()'s own, and a failing statement reaches the caller with the
 * push span closed against it. Whether the row actually disappears on
 * ROLLBACK is the server's own answer and stays in tests-integration/.
 */
final class SqlQueuePushOnTest extends TestCase
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

    public function test_push_on_inserts_through_the_supplied_transaction_and_leaves_the_constructor_link_untouched(): void
    {
        $link = new RecordingSqlLink();
        $transaction = new RecordingSqlTransaction();
        $queue = new SqlQueue($link);

        $queue->pushOn($transaction, new RecordingJob('hello'), queue: 'reports', maxAttempts: 3);

        self::assertSame([], $link->executed, 'the constructor link must see no statement at all');
        self::assertCount(1, $transaction->executed);

        [$sql, $params] = $transaction->executed[0];

        self::assertStringStartsWith('INSERT INTO kinetis_queue_jobs ', $sql);
        self::assertSame(RecordingJob::class, $params[0]);
        self::assertSame('reports', $params[2]);
        self::assertSame(3, $params[4]);
    }

    /**
     * The transaction stays the caller's to end: pushOn() runs the one
     * statement and makes none of the lifecycle calls that would decide
     * the surrounding unit of work's outcome for it. beginTransaction()
     * throws on this fixture, so a nested transaction would fail here
     * rather than be counted.
     */
    public function test_push_on_neither_ends_nor_nests_the_supplied_transaction(): void
    {
        $transaction = new RecordingSqlTransaction();

        (new SqlQueue(new RecordingSqlLink()))->pushOn($transaction, new RecordingJob('hello'));

        self::assertSame(0, $transaction->commits);
        self::assertSame(0, $transaction->rollbacks);
        self::assertSame(0, $transaction->closes);
    }

    /**
     * The transaction is an argument, not a link the queue adopts: the
     * very next push() goes back to the constructor link, and the
     * transaction — which the caller may already have committed —
     * receives nothing more.
     */
    public function test_push_on_does_not_retain_the_transaction_for_later_pushes(): void
    {
        $link = new RecordingSqlLink();
        $transaction = new RecordingSqlTransaction();
        $queue = new SqlQueue($link);

        $queue->pushOn($transaction, new RecordingJob('enlisted'));
        $queue->push(new RecordingJob('ordinary'));

        self::assertCount(1, $transaction->executed);
        self::assertCount(1, $link->executed);
    }

    /**
     * One insertion path, proven from the outside: the same arguments
     * produce the same statement and the same bound envelope whichever
     * of the two entry points wrote it. The two timestamp columns are
     * compared for shape only — both are read from `time()` at the
     * moment of the call, so a second boundary between the two calls
     * would make equality a clock race rather than a contract.
     */
    public function test_push_on_writes_the_envelope_push_writes(): void
    {
        $link = new RecordingSqlLink();
        $transaction = new RecordingSqlTransaction();
        $queue = new SqlQueue($link);

        $job = new RecordingJob('hello');
        $queue->push($job, delaySeconds: 30, queue: 'reports', maxAttempts: 3);
        $queue->pushOn($transaction, $job, delaySeconds: 30, queue: 'reports', maxAttempts: 3);

        [$pushSql, $pushParams] = $link->executed[0];
        [$pushOnSql, $pushOnParams] = $transaction->executed[0];

        self::assertSame($pushSql, $pushOnSql);
        self::assertSame(
            self::withoutTimestamps($pushParams),
            self::withoutTimestamps($pushOnParams),
        );

        foreach ([3, 6] as $timestampIndex) {
            self::assertMatchesRegularExpression(
                '/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/',
                (string) $pushOnParams[$timestampIndex],
            );
        }
    }

    /**
     * Not merely "pushOn() also rejects this": the same exception, with
     * the same message, out of the same validation call — and no
     * statement reaching either surface, so a rejected enqueue cannot
     * have written into the caller's transaction.
     *
     * @param array{int, string, ?int} $arguments
     */
    #[DataProvider('invalidPushArguments')]
    public function test_push_on_rejects_what_push_rejects(array $arguments): void
    {
        [$delaySeconds, $queueName, $maxAttempts] = $arguments;

        $link = new RecordingSqlLink();
        $transaction = new RecordingSqlTransaction();
        $queue = new SqlQueue($link);

        $fromPush = self::failureOf(
            static fn () => $queue->push(new RecordingJob('rejected'), $delaySeconds, $queueName, $maxAttempts),
        );
        $fromPushOn = self::failureOf(
            static fn () => $queue->pushOn($transaction, new RecordingJob('rejected'), $delaySeconds, $queueName, $maxAttempts),
        );

        self::assertInstanceOf(InvalidQueueArgumentException::class, $fromPushOn);
        self::assertSame($fromPush::class, $fromPushOn::class);
        self::assertSame($fromPush->getMessage(), $fromPushOn->getMessage());
        self::assertSame([], $link->executed);
        self::assertSame([], $transaction->executed, 'a rejected enqueue must write nothing into the caller transaction');
    }

    /**
     * @return iterable<string, array{array{int, string, ?int}}>
     */
    public static function invalidPushArguments(): iterable
    {
        yield 'negative delay' => [[-1, 'default', null]];
        yield 'empty queue name' => [[0, '', null]];
        yield 'malformed queue name' => [[0, 'not a queue name', null]];
        yield 'negative max attempts' => [[0, 'default', -1]];
    }

    /**
     * The caller sees the transaction's own failure, and the push span
     * closes against that exception rather than being left open or
     * reported as a success.
     */
    public function test_a_failing_enqueue_statement_propagates_and_closes_the_push_span_with_it(): void
    {
        $telemetry = new ThrowingTelemetry();
        Telemetry::global()->swap($telemetry);

        $failure = new RuntimeException('the transaction refused the INSERT');
        $transaction = new RecordingSqlTransaction();
        $transaction->failExecuteWith = $failure;

        $thrown = self::failureOf(
            fn () => (new SqlQueue(new RecordingSqlLink()))->pushOn($transaction, new RecordingJob('hello')),
        );

        self::assertSame($failure, $thrown);
        self::assertSame(1, $telemetry->jobPushEndedCalls);
        self::assertSame($failure, $telemetry->jobPushEndedFailure);
    }

    /**
     * A successful enqueue closes the same span with no failure, which
     * is what makes the assertion above discriminating rather than a
     * restatement of "jobPushEnded() was called".
     */
    public function test_a_successful_enqueue_closes_the_push_span_without_a_failure(): void
    {
        $telemetry = new ThrowingTelemetry();
        Telemetry::global()->swap($telemetry);

        (new SqlQueue(new RecordingSqlLink()))->pushOn(new RecordingSqlTransaction(), new RecordingJob('hello'));

        self::assertSame(1, $telemetry->jobPushEndedCalls);
        self::assertNull($telemetry->jobPushEndedFailure);
    }

    /**
     * @param array<int|string, mixed> $params
     * @return array<int|string, mixed>
     */
    private static function withoutTimestamps(array $params): array
    {
        unset($params[3], $params[6]);

        return $params;
    }

    private static function failureOf(callable $call): Throwable
    {
        try {
            $call();
        } catch (Throwable $e) {
            return $e;
        }

        self::fail('Expected the call to throw.');
    }
}
