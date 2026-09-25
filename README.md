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

Part of [Kinetis](https://kinetis.dev/), a non-blocking PHP framework for
API-first applications, developed in the
[kinetis-dev/kinetis](https://github.com/kinetis-dev/kinetis) monorepo.

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

`SqlQueue` declares `Kinetis\Queue\ClearableQueueInterface`. Clearing
deletes every row on the queue whose `reserved_at` is null, and reports
how many the `DELETE` removed. That is narrower than what `size()`
counts: an expired reservation — one older than
`QUEUE_VISIBILITY_TIMEOUT_SECONDS` — counts as waiting and `pop()` may
reclaim it, but `clear()` still leaves it alone — the worker holding it
may simply be slow, and still has a settlement to make.

Every reservation and every timeout reclaim writes a fresh random
`reserved_token`, and `ack()`/`release()`/`fail()` match on the row id
*and* that token. A settlement arriving after another worker reclaimed
the row therefore writes nothing and raises
`Kinetis\Queue\Exception\StaleJobHandleException`, which `queue:work`
reports as a lost delivery instead of settling somebody else's.

`SqlQueue` also declares `Kinetis\Queue\RenewableQueueInterface`, so
`queue:work` restamps a running job's `reserved_at` at half
`QUEUE_VISIBILITY_TIMEOUT_SECONDS` — one `UPDATE` under the same row-id
and token predicate, touching neither `attempts` nor `available_at`. The
setting therefore sizes crash recovery, not job duration. A reservation
still expires under a job whose worker died and under a handler that
never yields to the event loop, so keep handlers idempotent: fencing
keeps a late settlement from doing damage, it does not stop the job from
running twice.

## Enqueueing inside your own transaction

`push()` runs its `INSERT` on the queue's own connection, so the job is
enqueued even when a transaction the caller is inside later rolls back.
`pushOn()` places the row on a transaction you already hold instead:

```php
use Kinetis\Persistence\Contract\SqlTransaction;

$guard->transaction($link, function (SqlTransaction $tx) use ($queue, $orderId): void {
    $tx->execute('UPDATE orders SET status = ? WHERE id = ?', ['paid', $orderId]);

    $queue->pushOn($tx, new SendReceipt($orderId));
});
```

The row becomes visible and durable only if that transaction commits. A
throw before the commit rolls it back with the rest of the work, and a
`COMMIT` that fails leaves the outcome unknown, the same as for every
other statement in the transaction. `pushOn()` runs one statement on the
transaction you give it and nothing else — it never commits, rolls back,
nests, or reaches for the queue's own connection, so ending the
transaction stays yours.

The transaction must address the database holding `kinetis_queue_jobs`.
The queue connection's own name picks the connection behind `push()`
and does not redirect a transaction you supply.

This is `kinetis/queue-sql`'s own API, not part of `QueueInterface`, so
it needs a `SqlQueue` rather than the interface the container binds.
`SqlQueueFactory::fromConfig()` returns that class. Build it once and
register that one object under both ids:

```php
use Kinetis\Queue\QueueInterface;
use Kinetis\QueueSql\SqlQueue;
use Kinetis\QueueSql\SqlQueueFactory;

$queue = SqlQueueFactory::fromConfig($config);

$app->instance(SqlQueue::class, $queue);
$app->instance(QueueInterface::class, $queue);
$app->onDispose($queue->dispose(...));
```

Ordinary `QueueInterface` consumers and `pushOn()` callers then share one
backend instance and its one connection pool. Binding only
`SqlQueue::class` leaves the default `QueueInterface` binding in place,
and it builds a second `SqlQueue` with a pool of its own.

The factory opened that connection, so the queue owns it and the
`onDispose()` line closes it when the worker ends. A `SqlQueue`
constructed directly around a link you already have closes nothing: the
link stays yours. See
[kinetis.dev/docs/appendix-queue.html](https://kinetis.dev/docs/appendix-queue.html)'s
"Connection ownership".

`pushOn()` takes a raw `Kinetis\Persistence\Contract\SqlTransaction`;
an ORM transaction session does not expose its transaction. If ORM work
must schedule a job atomically, map an application outbox intent as an
entity with a `#[BelongsTo]` to what it depends on, persist both and
flush once, then publish after the transaction returns — see
[kinetis.dev/docs/orm.html](https://kinetis.dev/docs/orm.html#locking-rows-or-entities-and-sql-in-one-transaction)'s
"Locking rows, or entities and SQL in one transaction".

## Configuration

```
QUEUE_CONNECTION=sql
DB_CONNECTION=mysql
DB_HOST=127.0.0.1
DB_NAME=app
DB_USER=app
DB_PASSWORD=secret
```

`DB_*` are the exact keys [`kinetis/database-bridge`](https://github.com/kinetis-dev/database-bridge) reads. The one
key this package introduces itself:

| Key | Default | Purpose |
|---|---|---|
| `QUEUE_VISIBILITY_TIMEOUT_SECONDS` | `300` | Seconds before a crashed worker's reserved job becomes poppable again; `queue:work` renews a running job's reservation at half this. Must be a positive integer. |

Both are scoped by the queue connection's own name, the same way every
other backend's keys are; see
[kinetis.dev/docs/queue-sql.html#named-connections](https://kinetis.dev/docs/queue-sql.html#named-connections).
[`kinetis/queue`](https://github.com/kinetis-dev/queue)'s own keys (`QUEUE_CONNECTION`,
`QUEUE_MAX_ATTEMPTS`, ...) are documented in that package; full
reference:
[kinetis.dev/docs/config.html](https://kinetis.dev/docs/config.html).

## Installation

```sh
composer require kinetis/queue-sql
```

Requires PHP 8.4+, [`kinetis/framework`](https://github.com/kinetis-dev/framework), [`kinetis/queue`](https://github.com/kinetis-dev/queue),
[`kinetis/persistence`](https://github.com/kinetis-dev/persistence), and
[`kinetis/database-bridge`](https://github.com/kinetis-dev/database-bridge). Full documentation:
[kinetis.dev/docs/queue-sql.html](https://kinetis.dev/docs/queue-sql.html).

## License

MIT — see [LICENSE](LICENSE).
