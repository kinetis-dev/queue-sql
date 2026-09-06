<?php

declare(strict_types=1);

namespace Kinetis\QueueSql\Tests;

use Kinetis\Queue\JobSerializer;
use Kinetis\QueueSql\SqlQueue;
use Kinetis\QueueSql\Tests\Fixtures\Priority;
use Kinetis\QueueSql\Tests\Fixtures\RecordingSqlLink;
use Kinetis\QueueSql\Tests\Fixtures\RichPayloadJob;
use PHPUnit\Framework\TestCase;

/**
 * push()'s INSERT carries a JobSerializer::serialize() payload through
 * json_encode() with every value's type intact — a float above all,
 * since without JSON_PRESERVE_ZERO_FRACTION an integral-valued float
 * round-trips back as an int. RecordingSqlLink captures the bound
 * params without a live database. pop()'s decode side and
 * reserveNext()'s query stay real-backend-only; see SqlQueueTest.
 */
final class SqlQueuePushEnvelopeTest extends TestCase
{
    public function test_push_encodes_args_preserving_float_type_via_json_preserve_zero_fraction(): void
    {
        $link = new RecordingSqlLink();
        $queue = new SqlQueue($link);

        $queue->push(new RichPayloadJob(4.0, [['id' => 1, 'tags' => ['a', 'b']]], Priority::High));

        self::assertCount(1, $link->executed);

        [, $params] = $link->executed[0];
        $argsJson = $params[1];

        self::assertIsString($argsJson);
        self::assertStringContainsString('"ratio":4.0', $argsJson, 'JSON_PRESERVE_ZERO_FRACTION is what keeps this "4.0" instead of "4"');

        $decoded = json_decode($argsJson, true, flags: JSON_THROW_ON_ERROR);

        self::assertIsFloat($decoded['ratio']);
        self::assertSame(4.0, $decoded['ratio']);
        self::assertSame([['id' => 1, 'tags' => ['a', 'b']]], $decoded['items']);
        self::assertSame(Priority::High->value, $decoded['priority']);
    }

    /**
     * The bytes push() sent reconstruct into a type-correct object
     * through serialize() → json_encode() → json_decode() →
     * deserializeJob(), not merely a plausible-looking array.
     */
    public function test_the_encoded_args_reconstruct_into_an_equivalent_job(): void
    {
        $link = new RecordingSqlLink();
        $queue = new SqlQueue($link);

        $original = new RichPayloadJob(4.0, [['id' => 1, 'tags' => []]], Priority::Low);
        $queue->push($original);

        [, $params] = $link->executed[0];
        $class = $params[0];
        $args = json_decode((string) $params[1], true, flags: JSON_THROW_ON_ERROR);

        $restored = JobSerializer::deserializeJob($class, $args);

        self::assertEquals($original, $restored);
    }
}
