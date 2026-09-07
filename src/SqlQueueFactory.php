<?php

declare(strict_types=1);

namespace Kinetis\QueueSql;

use InvalidArgumentException;
use Kinetis\Config\Config;
use Kinetis\Persistence\SqlConnectionFactory;
use Kinetis\Queue\ClearableQueueInterface;

/**
 * Builds the SQL queue backend `QUEUE_CONNECTION=sql` selects — called
 * by `kinetis/queue`'s own `QueueFactory::fromConfig()`, gated behind a
 * `class_exists()` check so core never depends on this package
 * directly, the same pattern used for every other optional queue
 * backend (`kinetis/queue-sqs`, `kinetis/queue-rabbitmq`).
 *
 * Returns `ClearableQueueInterface`, the capability this backend
 * declares; see `QueueFactory` for why the connection-driven factory
 * stays on `QueueInterface`.
 */
final class SqlQueueFactory
{
    /**
     * The same default `kinetis/queue-redis` applies, so both
     * application-owned reservation backends recover a crashed worker's
     * job on identical terms.
     */
    private const int DEFAULT_VISIBILITY_TIMEOUT_SECONDS = 300;

    public static function fromConfig(Config $config, string $connectionName = 'default'): ClearableQueueInterface
    {
        return new SqlQueue(
            SqlConnectionFactory::fromConfig($config, $connectionName),
            self::visibilityTimeoutSeconds($config, $connectionName),
        );
    }

    /**
     * A reservation is finite, so this setting has a real default rather
     * than an "off" state: an absent value still reclaims a crashed
     * worker's row.
     */
    private static function visibilityTimeoutSeconds(Config $config, string $connectionName): int
    {
        $key = Config::scopedKey('QUEUE_VISIBILITY_TIMEOUT_SECONDS', $connectionName);
        $seconds = $config->int($key, self::DEFAULT_VISIBILITY_TIMEOUT_SECONDS);

        if ($seconds < 1) {
            throw new InvalidArgumentException("{$key} must be a positive number of seconds, got {$seconds}.");
        }

        return $seconds;
    }
}
