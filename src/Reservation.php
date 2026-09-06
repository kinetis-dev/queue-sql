<?php

declare(strict_types=1);

namespace Kinetis\QueueSql;

/**
 * The delivery receipt SqlQueue puts on QueuedJob::$handle: the row, plus
 * the token identifying the one reservation this delivery came from.
 *
 * A row id alone names the job, not the delivery — after a visibility
 * timeout hands the row to another worker, the earlier worker's id still
 * matches a row somebody else now holds. The token is written under the
 * row lock on every reservation and reclaim, so a settlement predicated
 * on both settles only the delivery it belongs to.
 *
 * Opaque to callers: only SqlQueue reads either field, and a QueuedJob is
 * handed back to ack()/release()/fail() unmodified.
 */
final readonly class Reservation
{
    public function __construct(
        /** Whatever the driver reports the primary key as — an int, or a decimal string past PHP_INT_MAX. */
        public mixed $id,
        public string $token,
    ) {}
}
