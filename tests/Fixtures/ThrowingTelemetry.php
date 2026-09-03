<?php

declare(strict_types=1);

namespace Kinetis\QueueSql\Tests\Fixtures;

use Kinetis\Instrumentation\TelemetryInterface;
use RuntimeException;
use Throwable;

/**
 * Every hook is a no-op, matching NullTelemetry, except jobPushEnded()/
 * jobPushStarted() — each toggleable to throw, so a test can prove
 * SqlQueue::push()'s real INSERT is unaffected by a broken telemetry
 * backend without needing a real database. throwOnJobPushStarted also
 * doubles as an ordering proof: push() argument validation must run
 * before jobPushStarted() is ever called at all, so a rejected push()
 * must throw the validation exception, never this fixture's own.
 */
final class ThrowingTelemetry implements TelemetryInterface
{
    public bool $throwOnJobPushEnded = false;

    public bool $throwOnJobPushStarted = false;

    #[\Override]
    public function phase(string $name, float $startedAt, float $endedAt): void {}

    #[\Override]
    public function routeMatchStarted(string $method, string $path): mixed
    {
        return null;
    }

    #[\Override]
    public function routeMatchEnded(mixed $token, ?string $pattern): void {}

    #[\Override]
    public function middlewareEntered(string $class): mixed
    {
        return null;
    }

    #[\Override]
    public function middlewareExited(mixed $token, ?Throwable $failure): void {}

    #[\Override]
    public function hydrationStarted(string $dtoClass): mixed
    {
        return null;
    }

    #[\Override]
    public function hydrationEnded(mixed $token): void {}

    #[\Override]
    public function controllerInvoked(string $class, string $method): mixed
    {
        return null;
    }

    #[\Override]
    public function controllerReturned(mixed $token, ?Throwable $failure): void {}

    #[\Override]
    public function responseEncodingStarted(): mixed
    {
        return null;
    }

    #[\Override]
    public function responseEncodingEnded(mixed $token): void {}

    #[\Override]
    public function queryDispatched(string $system, string $sql): mixed
    {
        return null;
    }

    #[\Override]
    public function queryServerStarted(mixed $token): void {}

    #[\Override]
    public function queryReaped(mixed $token, ?Throwable $failure): void {}

    #[\Override]
    public function transactionStarted(string $system): mixed
    {
        return null;
    }

    #[\Override]
    public function transactionEnded(mixed $token, string $outcome): void {}

    #[\Override]
    public function taskBatchStarted(int $count): mixed
    {
        return null;
    }

    #[\Override]
    public function taskBatchEnded(mixed $token): void {}

    #[\Override]
    public function taskStarted(int $index): mixed
    {
        return null;
    }

    #[\Override]
    public function taskEnded(mixed $token, ?Throwable $failure): void {}

    #[\Override]
    public function eventDispatched(string $eventClass): mixed
    {
        return null;
    }

    #[\Override]
    public function eventSettled(mixed $token): void {}

    #[\Override]
    public function listenerInvoked(string $listenerClass, string $method): mixed
    {
        return null;
    }

    #[\Override]
    public function listenerReturned(mixed $token, ?Throwable $failure): void {}

    #[\Override]
    public function toolCallStarted(string $tool): mixed
    {
        return null;
    }

    #[\Override]
    public function toolCallEnded(mixed $token, ?Throwable $failure): void {}

    #[\Override]
    public function resourceReadStarted(string $uri): mixed
    {
        return null;
    }

    #[\Override]
    public function resourceReadEnded(mixed $token): void {}

    #[\Override]
    public function jobPushStarted(string $jobClass, string $queue): mixed
    {
        if ($this->throwOnJobPushStarted) {
            throw new RuntimeException('The telemetry backend failed to start a job-push span.');
        }

        return null;
    }

    /**
     * @return array<string, string>
     */
    #[\Override]
    public function jobPushMetadata(mixed $token): array
    {
        return [];
    }

    #[\Override]
    public function jobPushEnded(mixed $token, ?Throwable $failure): void
    {
        if ($this->throwOnJobPushEnded) {
            throw new RuntimeException('The telemetry backend failed to finish a job-push span.');
        }
    }

    /**
     * @param array<string, string> $metadata
     */
    #[\Override]
    public function jobStarted(string $jobClass, string $queue, int $attempt, array $metadata = []): mixed
    {
        return null;
    }

    #[\Override]
    public function jobFinished(mixed $token, string $outcome, ?Throwable $failure): void {}
}
