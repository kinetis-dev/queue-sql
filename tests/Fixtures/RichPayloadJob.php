<?php

declare(strict_types=1);

namespace Kinetis\QueueSql\Tests\Fixtures;

use Kinetis\Queue\Job;

/**
 * One constructor argument per rich/nested wire shape — used to prove
 * SqlQueue's own real push() INSERT carries a
 * JobSerializer::serialize()-normalized payload through its own
 * json_encode() call losslessly, floats included.
 */
final readonly class RichPayloadJob implements Job
{
    /**
     * @param list<array<string, mixed>> $items
     */
    public function __construct(
        public float $ratio,
        public array $items,
        public Priority $priority,
    ) {}

    public function handle(): void
    {
    }
}
