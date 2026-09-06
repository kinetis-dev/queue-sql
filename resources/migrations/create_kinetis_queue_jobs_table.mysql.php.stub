<?php

declare(strict_types=1);

use Kinetis\Persistence\Contract\MysqlLink;
use Kinetis\Persistence\Contract\PostgresLink;
use Kinetis\Migrations\Migration;

// MySQL version. Copy this file into your own migrations/ directory (see
// kinetis/migrations), renaming it with a timestamp prefix, e.g.
// 20260810143000_create_kinetis_queue_jobs_table.php.
// Required by Kinetis\Queue\SqlQueue.
//
// AUTO_INCREMENT/UNSIGNED are MySQL-only syntax — see the .pgsql. sibling
// stub for the Postgres equivalent; migrations are raw SQL by design, so
// there is no shared, dialect-abstracted version of this file.
//
// `queue` and `reserved_token` are ascii_bin: MySQL's default collation
// compares case-insensitively, and both columns are matched for exact
// equality — a queue name the caller asked for, and the token fencing a
// settlement to the reservation that produced it. Postgres compares
// byte-exactly already.

return new class implements Migration
{
    public function up(MysqlLink|PostgresLink $db): void
    {
        $db->execute(<<<'SQL'
            CREATE TABLE kinetis_queue_jobs (
                id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                class VARCHAR(255) NOT NULL,
                queue VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'default',
                args TEXT NOT NULL,
                available_at TIMESTAMP NOT NULL,
                reserved_at TIMESTAMP NULL,
                reserved_token VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NULL,
                attempts INT UNSIGNED NOT NULL DEFAULT 0,
                max_attempts INT UNSIGNED NULL,
                metadata TEXT NULL,
                created_at TIMESTAMP NOT NULL,
                INDEX kinetis_queue_jobs_queue_available_at_index (queue, available_at, reserved_at)
            )
            SQL);
    }

    public function down(MysqlLink|PostgresLink $db): void
    {
        $db->execute('DROP TABLE kinetis_queue_jobs');
    }
};
