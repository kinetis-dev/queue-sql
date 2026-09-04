<?php

declare(strict_types=1);

namespace Kinetis\QueueSql;

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
    public static function fromConfig(Config $config, string $connectionName = 'default'): ClearableQueueInterface
    {
        // QUEUE_VISIBILITY_TIMEOUT_SECONDS has no default — absent means
        // SqlQueue's own behavior of a crashed worker's row staying
        // reserved forever, unchanged unless explicitly opted into.
        $visibilityTimeoutSeconds = $config->intOrNull(Config::scopedKey('QUEUE_VISIBILITY_TIMEOUT_SECONDS', $connectionName));

        return new SqlQueue(SqlConnectionFactory::fromConfig($config, $connectionName), $visibilityTimeoutSeconds);
    }
}
