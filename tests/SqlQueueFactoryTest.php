<?php

declare(strict_types=1);

namespace Kinetis\QueueSql\Tests;

use InvalidArgumentException;
use Kinetis\Config\Config;
use Kinetis\Config\Exception\MissingConfigException;
use Kinetis\Queue\ClearableQueueInterface;
use Kinetis\QueueSql\SqlQueue;
use Kinetis\QueueSql\SqlQueueFactory;
use Kinetis\QueueSql\Tests\Fixtures\RecordingJob;
use Kinetis\QueueSql\Tests\Fixtures\RecordingSqlTransaction;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use ReflectionProperty;

/**
 * Construction only — ConnectionFactory::fromConfig() doesn't connect
 * eagerly (neither the native mysqli/pgsql drivers nor the PDO ones open
 * a socket in their own constructors), so this is safe to run with no
 * real database reachable. SqlQueue's own backend-specific correctness
 * is deliberately never unit-tested against a fake — see
 * tests-integration/.
 */
final class SqlQueueFactoryTest extends TestCase
{
    public function test_builds_a_queue_for_the_default_connection(): void
    {
        $config = new Config([
            'DB_CONNECTION' => 'mysql',
            'DB_PASSWORD' => 'secret',
        ]);

        self::assertInstanceOf(SqlQueue::class, SqlQueueFactory::fromConfig($config));
    }

    public function test_a_named_connection_reads_its_own_settings(): void
    {
        $config = new Config([
            'DB_REPORTS_CONNECTION' => 'pgsql',
            'DB_REPORTS_PASSWORD' => 'secret',
        ]);

        self::assertInstanceOf(SqlQueue::class, SqlQueueFactory::fromConfig($config, 'reports'));
    }

    public function test_a_missing_db_password_throws_a_clear_error(): void
    {
        $config = new Config(['DB_CONNECTION' => 'mysql']);

        $this->expectException(MissingConfigException::class);
        $this->expectExceptionMessage('DB_PASSWORD');
        SqlQueueFactory::fromConfig($config);
    }

    /**
     * No setting means the finite default, not an indefinite
     * reservation: a crashed worker's row is reclaimable out of the box.
     */
    public function test_an_absent_visibility_timeout_falls_back_to_the_finite_default(): void
    {
        $config = new Config([
            'DB_CONNECTION' => 'mysql',
            'DB_PASSWORD' => 'secret',
        ]);

        $queue = SqlQueueFactory::fromConfig($config);

        $property = new ReflectionProperty(SqlQueue::class, 'visibilityTimeoutSeconds');
        self::assertSame(300, $property->getValue($queue));
    }

    public function test_a_non_positive_visibility_timeout_is_rejected(): void
    {
        $config = new Config([
            'DB_CONNECTION' => 'mysql',
            'DB_PASSWORD' => 'secret',
            'QUEUE_VISIBILITY_TIMEOUT_SECONDS' => '0',
        ]);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('QUEUE_VISIBILITY_TIMEOUT_SECONDS must be a positive number of seconds, got 0.');
        SqlQueueFactory::fromConfig($config);
    }

    public function test_the_visibility_timeout_reaches_sqlqueue_as_a_real_integer(): void
    {
        $config = new Config([
            'DB_CONNECTION' => 'mysql',
            'DB_PASSWORD' => 'secret',
            'QUEUE_VISIBILITY_TIMEOUT_SECONDS' => '300',
        ]);

        $queue = SqlQueueFactory::fromConfig($config);

        $property = new ReflectionProperty(SqlQueue::class, 'visibilityTimeoutSeconds');
        self::assertSame(300, $property->getValue($queue));
    }

    /**
     * The declared return type, not merely the object that comes back:
     * an application binding `SqlQueue::class` to this result, and the
     * callers typed against that binding, reach `pushOn()` through the
     * signature alone. Widening this to an interface would leave every
     * assertInstanceOf() above passing while putting a runtime
     * narrowing check between a caller and the one API it named this
     * backend for.
     */
    public function test_the_declared_return_type_is_the_class_carrying_push_on(): void
    {
        $returnType = (new ReflectionMethod(SqlQueueFactory::class, 'fromConfig'))->getReturnType();

        self::assertNotNull($returnType);
        self::assertSame(SqlQueue::class, (string) $returnType);
    }

    /**
     * The concrete return type takes nothing away: the queue this
     * builds still satisfies the contracts `QueueFactory` and
     * `PackageBootstrap` hand out, and its enlisted push runs on the
     * transaction it is given rather than the connection built here —
     * which is what keeps this test offline.
     */
    public function test_the_built_queue_still_satisfies_the_shared_contracts_and_enlists_a_transaction(): void
    {
        $config = new Config([
            'DB_CONNECTION' => 'mysql',
            'DB_PASSWORD' => 'secret',
        ]);

        $queue = SqlQueueFactory::fromConfig($config);
        self::assertInstanceOf(ClearableQueueInterface::class, $queue);

        $transaction = new RecordingSqlTransaction();
        $queue->pushOn($transaction, new RecordingJob('hello'));

        self::assertCount(1, $transaction->executed);
        self::assertStringStartsWith('INSERT INTO kinetis_queue_jobs ', $transaction->executed[0][0]);
    }
}
