<?php

declare(strict_types=1);

namespace Kinetis\QueueSql\Tests;

use InvalidArgumentException;
use Kinetis\Config\Config;
use Kinetis\Config\Exception\MissingConfigException;
use Kinetis\QueueSql\SqlQueue;
use Kinetis\QueueSql\SqlQueueFactory;
use PHPUnit\Framework\TestCase;
use ReflectionProperty;

/**
 * Construction only — SqlConnectionFactory::fromConfig() doesn't connect
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
}
