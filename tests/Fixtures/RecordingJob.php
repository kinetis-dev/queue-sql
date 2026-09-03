<?php

declare(strict_types=1);

namespace Kinetis\QueueSql\Tests\Fixtures;

use Kinetis\Queue\Job;

/**
 * A minimal, real Job — push() only ever needs to serialize this via
 * reflection over its constructor, never actually invoke handle().
 */
final readonly class RecordingJob implements Job
{
    public function __construct(
        public string $message,
    ) {}

    public function handle(): void
    {
    }
}
