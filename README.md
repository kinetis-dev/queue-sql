<p align="center">
  <img src="logo.svg" alt="Kinetis" width="420">
</p>

<p align="center">
  <strong>kinetis/queue-sql</strong>
  <br>
  <strong>A SQL-backed (MySQL/Postgres) queue implementation for kinetis/queue's <code>QueueInterface</code></strong>
</p>

<p align="center">
  <a href="https://packagist.org/packages/kinetis/queue-sql"><img src="https://img.shields.io/packagist/v/kinetis/queue-sql?label=version" alt="Packagist Version"></a>
  <a href="https://packagist.org/packages/kinetis/queue-sql"><img src="https://img.shields.io/packagist/dt/kinetis/queue-sql" alt="Packagist Downloads"></a>
  <a href="https://packagist.org/packages/kinetis/queue-sql"><img src="https://img.shields.io/packagist/php-v/kinetis/queue-sql" alt="PHP Version"></a>
  <a href="https://packagist.org/packages/kinetis/queue-sql"><img src="https://img.shields.io/packagist/l/kinetis/queue-sql" alt="License"></a>
  <a href="https://github.com/kinetis-dev/kinetis/actions/workflows/ci.yml"><img src="https://github.com/kinetis-dev/kinetis/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
</p>

---

Adds MySQL/Postgres as a queue backend, riding an existing database
instead of a separate service. `push()`/`pop()`/`ack()`/`release()`/`fail()`
work exactly like any other backend — only your configuration changes.
`pop()` relies on `SELECT ... FOR UPDATE SKIP LOCKED` to guarantee two
workers never receive the same job — MySQL 8.0+ or MariaDB 10.6+.

```php
use Kinetis\Config\Config;
use Kinetis\QueueSql\SqlQueueFactory;

$queue = SqlQueueFactory::fromConfig($config);

$queue->push(new SendWelcomeEmail($email, $name), queue: 'default');
```

## The queue needs a table

Two ready-to-copy migration stubs, one per dialect:

```
vendor/kinetis/queue-sql/resources/migrations/create_kinetis_queue_jobs_table.mysql.php.stub
vendor/kinetis/queue-sql/resources/migrations/create_kinetis_queue_jobs_table.pgsql.php.stub
```

Copy whichever matches your database into your own `migrations/`
directory with a timestamp prefix, then run `vendor/bin/kinetis migrate`.

## Configuration

```
QUEUE_CONNECTION=sql
DB_CONNECTION=mysql
DB_HOST=127.0.0.1
DB_NAME=app
DB_USER=app
DB_PASSWORD=secret
```

`DB_*` are the exact keys `kinetis/persistence` already reads. The one
key this package introduces itself:

| Key | Default | Purpose |
|---|---|---|
| `QUEUE_VISIBILITY_TIMEOUT_SECONDS` | *(unset — never reclaimed)* | Seconds before a crashed worker's reserved job becomes poppable again. |

Both are scoped by `QUEUE_CONNECTION_NAME` the same way every other
backend's keys are. `kinetis/queue`'s own keys (`QUEUE_CONNECTION`,
`QUEUE_MAX_ATTEMPTS`, ...) are documented in that package; full
reference:
[kinetis.dev/docs/config.html](https://kinetis.dev/docs/config.html).

## Installation

```sh
composer require kinetis/queue-sql
```

Requires PHP 8.4+, `kinetis/framework`, `kinetis/queue`, and
`kinetis/persistence`. Full documentation:
[kinetis.dev/docs/queue-sql.html](https://kinetis.dev/docs/queue-sql.html).

## License

MIT — see [LICENSE](../../LICENSE).
