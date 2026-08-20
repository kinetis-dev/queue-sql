<?php

declare(strict_types=1);

namespace Kinetis\QueueSql;

use Kinetis\Config\Config;
use Kinetis\Persistence\SqlConnectionFactory;
use Kinetis\Queue\QueueInterface;

/**
 * Builds the SQL queue backend `QUEUE_CONNECTION=sql` selects — called
 * by `kinetis/queue`'s own `QueueFactory::fromConfig()`, gated behind a
 * `class_exists()` check so core never depends on this package
 * directly, the same pattern used for every other optional queue
 * backend (`kinetis/queue-sqs`, `kinetis/queue-rabbitmq`).
 */
final class SqlQueueFactory
{
    public static function fromConfig(Config $config, string $connectionName = 'default'): QueueInterface
    {
        // QUEUE_VISIBILITY_TIMEOUT_SECONDS has no default — absent means
        // SqlQueue's own behavior of a crashed worker's row staying
        // reserved forever, unchanged unless explicitly opted into.
        $visibilityTimeoutRaw = $config->string(Config::scopedKey('QUEUE_VISIBILITY_TIMEOUT_SECONDS', $connectionName), '');
        $visibilityTimeoutSeconds = $visibilityTimeoutRaw === '' ? null : (int) $visibilityTimeoutRaw;

        return new SqlQueue(SqlConnectionFactory::fromConfig($config, $connectionName), $visibilityTimeoutSeconds);
    }
}
