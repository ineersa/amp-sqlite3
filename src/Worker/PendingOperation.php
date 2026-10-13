<?php

declare(strict_types=1);

/*
 * This file is part of the fabpot/amphp-sqlite3 package.
 *
 * (c) Fabien Potencier <fabien@potencier.org>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Fabpot\Amp\Sqlite\Worker;

use Amp\Cancellation;
use Amp\CompositeCancellation;
use Fabpot\Amp\Sqlite\Internal\PipelineOperation;
use Fabpot\Amp\Sqlite\Worker\DTO\OperationResultDTO;

/** Explicit result ownership. Await or discard to release retained admission credit. */
final class PendingOperation
{
    private readonly string $subscription;
    private bool $consumed = false;

    /** @param \Closure(): void $cancelQueued
     * @param \Closure(): void $onConsumed
     * @internal Constructed only after bounded admission.
     */
    public function __construct(
        private readonly PipelineOperation $operation,
        private readonly Cancellation $cancellation,
        private readonly \Closure $cancelQueued,
        private readonly \Closure $onConsumed,
    ) {
        $this->subscription = $cancellation->subscribe($cancelQueued);
    }

    public function __destruct()
    {
        $this->discard();
    }

    /** The optional cancellation tightens the cancellation supplied at submission. */
    public function await(?Cancellation $cancellation = null): OperationResultDTO
    {
        $budget = $cancellation === null ? $this->cancellation : new CompositeCancellation($this->cancellation, $cancellation);
        try {
            if ($this->operation->result->isComplete()) {
                return $this->operation->result->getFuture()->await();
            }
            return $this->operation->result->getFuture()->await($budget);
        } finally {
            $this->discard();
        }
    }

    public function isComplete(): bool
    {
        return $this->operation->result->isComplete();
    }

    /** Validated completion, independent of caller cancellation. This does not consume the handle.
     * Call discard() in finally after observing completion. Undispatched cancellation still errors this future.
     * @return \Amp\Future<OperationResultDTO>
     */
    public function getFuture(): \Amp\Future
    {
        return $this->operation->result->getFuture();
    }

    /** Abandons the reply, not already-dispatched SQL. */
    public function discard(): void
    {
        if (!$this->consumed) {
            $this->consumed = true;
            $this->cancellation->unsubscribe($this->subscription);
            ($this->cancelQueued)();
            ($this->onConsumed)();
        }
    }
}
