<?php

declare(strict_types=1);

namespace Kinetis\QueueSql\Tests\Fixtures;

enum Priority: string
{
    case High = 'high';
    case Low = 'low';
}
