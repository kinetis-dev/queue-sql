<?php

declare(strict_types=1);

namespace Kinetis\QueueSql\Tests;

use Kinetis\Config\Config;
use Kinetis\Container\AppScope;
use Kinetis\Persistence\Contract\SqlLink;
use Kinetis\Queue\DisposableQueueInterface;
use Kinetis\Queue\PackageBootstrap;
use Kinetis\Queue\QueueInterface;
use Kinetis\QueueSql\SqlQueue;
use Kinetis\QueueSql\SqlQueueFactory;
use Kinetis\QueueSql\Tests\Fixtures\RecordingSqlLink;
use PHPUnit\Framework\TestCase;
use ReflectionProperty;

/**
 * Who closes the link, and when. Nothing here connects: neither the
 * driver constructors nor close() open a socket, so the whole file runs
 * with no database reachable — which is also what makes it the check
 * that disposal is safe before the queue's first statement.
 */
final class SqlQueueDisposalTest extends TestCase
{
    public function test_the_factory_hands_the_queue_the_link_it_opened(): void
    {
        $queue = SqlQueueFactory::fromConfig(self::config());
        $link = self::link($queue);

        self::assertFalse($link->isClosed());

        $queue->dispose();

        self::assertTrue($link->isClosed(), 'the link this factory opened is the queue\'s to close');
    }

    public function test_disposing_twice_closes_the_factory_link_once(): void
    {
        $queue = SqlQueueFactory::fromConfig(self::config());

        $queue->dispose();
        $queue->dispose();

        self::assertTrue(self::link($queue)->isClosed());
    }

    /**
     * A link the caller built stays the caller's: the queue closes
     * nothing it was merely lent, so a second consumer of that link
     * keeps working after the queue is finished with it.
     */
    public function test_a_directly_constructed_queue_leaves_the_caller_s_link_open(): void
    {
        $link = new RecordingSqlLink();

        new SqlQueue($link)->dispose();

        self::assertSame(0, $link->closeCalls);
    }

    public function test_a_caller_can_hand_the_queue_a_link_to_own(): void
    {
        $link = new RecordingSqlLink();
        $queue = new SqlQueue($link, 300, $link->close(...));

        $queue->dispose();
        $queue->dispose();

        self::assertSame(1, $link->closeCalls);
    }

    /**
     * End to end through the binding an application actually gets:
     * nothing is registered until something injects the queue, and the
     * worker's own teardown is what closes the connection the binding
     * opened.
     */
    public function test_the_package_bootstrap_closes_the_queue_it_built_when_the_worker_ends(): void
    {
        $app = new AppScope();
        new PackageBootstrap()->register($app, new Config([
            'QUEUE_CONNECTION' => 'sql',
            'DB_CONNECTION' => 'mysql',
            'DB_PASSWORD' => 'secret',
        ]));
        $app->boot();

        self::assertSame([], self::disposeCallbacks($app));

        $queue = $app->get(QueueInterface::class);
        self::assertInstanceOf(SqlQueue::class, $queue);
        self::assertInstanceOf(DisposableQueueInterface::class, $queue);
        self::assertCount(1, self::disposeCallbacks($app), 'one close for the one backend built');

        $app->dispose();

        self::assertTrue(self::link($queue)->isClosed());
    }

    private static function config(): Config
    {
        return new Config([
            'DB_CONNECTION' => 'mysql',
            'DB_PASSWORD' => 'secret',
        ]);
    }

    private static function link(SqlQueue $queue): SqlLink
    {
        /** @var SqlLink */
        return new ReflectionProperty(SqlQueue::class, 'db')->getValue($queue);
    }

    /**
     * @return list<callable(): void>
     */
    private static function disposeCallbacks(AppScope $app): array
    {
        /** @var list<callable(): void> */
        return new ReflectionProperty(AppScope::class, 'disposeCallbacks')->getValue($app);
    }
}
