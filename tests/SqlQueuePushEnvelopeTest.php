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
 * The real mechanism push()'s INSERT relies on: a
 * JobSerializer::serialize()-normalized payload survives push()'s own
 * json_encode() call (JSON_PRESERVE_ZERO_FRACTION included) with every
 * value's exact type intact — a float most notably, since without that
 * flag an integral-valued float silently round-trips back as an int.
 * RecordingSqlLink (shared with SqlQueuePushTelemetryTest) captures the
 * real INSERT's params without a live database — push() never reads the
 * row back, so nothing beyond capturing the bound params is needed
 * here. Still deliberately not proving pop()'s
 * own decode side, or reserveNext()'s query correctness — that stays
 * real-backend-only, per SqlQueueTest's own docblock.
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
        self::assertSame(
            [['$kinetisWireType' => 'map', 'entries' => ['id' => 1, 'tags' => ['a', 'b']]]],
            $decoded['items'],
        );
        self::assertSame(
            ['$kinetisWireType' => 'enum', 'class' => Priority::class, 'value' => 'high'],
            $decoded['priority'],
        );
    }

    /**
     * The same check against the shared serialize() → json_encode() →
     * json_decode() → JobSerializer::deserialize()/restore() path,
     * confirming the exact bytes push() actually sent reconstruct back
     * into a real, type-correct object — not just a plausible-looking
     * array.
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
