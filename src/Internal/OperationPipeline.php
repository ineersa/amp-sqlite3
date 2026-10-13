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

namespace Fabpot\Amp\Sqlite\Internal;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Parallel\Context\Context;
use Fabpot\Amp\Sqlite\Worker\DTO\PipelineLimitsDTO;
use Fabpot\Amp\Sqlite\Worker\Exception\PipelineCapacityException;
use Fabpot\Amp\Sqlite\Worker\Mode;
use Fabpot\Amp\Sqlite\Worker\PendingOperation;

use function Amp\async;

/** Exclusive transport owner. The child still executes complete operations sequentially. */
final class OperationPipeline
{
    private PipelineState $state = PipelineState::Running;
    /** @var list<QueuedPipelineOperation> */
    private array $queue = [];
    /** @var array<int, PipelineOperation> */
    private array $flight = [];
    /** @var array<int, PipelineOperation> */
    private array $admitted = [];
    private int $admittedBytes = 0;
    private int $flightBytes = 0;
    private int $flightCount = 0;
    private int $lastUsedAt;
    /** @var DeferredFuture<null> */
    private DeferredFuture $signal;
    /** @var DeferredFuture<null> */
    private readonly DeferredFuture $shutdown;
    private readonly DeferredCancellation $readerCancellation;
    private bool $closingConnection = false;
    private bool $joinStarted = false;
    /** @var DeferredFuture<null> */
    private readonly DeferredFuture $joined;
    /** @var DeferredFuture<null> */
    private DeferredFuture $readerSignal;

    /** @param Context<null, mixed, array<string, mixed>> $context */
    public function __construct(private readonly Context $context, private readonly PipelineLimitsDTO $limits, private int $nextId, private readonly \Closure $releaseTransport, private readonly \Closure $onFailure)
    {
        $this->signal = new DeferredFuture();
        $this->readerSignal = new DeferredFuture();
        $this->lastUsedAt = \time();
        $this->shutdown = new DeferredFuture();
        $this->shutdown->getFuture()->ignore();
        $this->joined = new DeferredFuture();
        $this->joined->getFuture()->ignore();
        $this->readerCancellation = new DeferredCancellation();
        async($this->write(...))->ignore();
        async($this->read(...))->ignore();
    }

    /** @param array<string, mixed> $request */
    public function submit(array $request, Mode $mode, int $maximumResultBytes, Cancellation $cancellation): PendingOperation
    {
        $cancellation->throwIfRequested();
        if ($this->state !== PipelineState::Running) {
            throw new WorkerFailure('Worker operation lane is closed');
        }
        $replyBytes = \max($maximumResultBytes + PipelineLimitsDTO::CHANNEL_OVERHEAD_BYTES, $this->limits->maximumErrorReplyBytes);
        // PHP serialization is the existing operation boundary format. Reserve framing and id metadata separately.
        $credit = \strlen(\serialize($request)) + PipelineLimitsDTO::CHANNEL_OVERHEAD_BYTES + $replyBytes;
        if ($credit > $this->limits->maximumInFlightBytes) {
            throw new PipelineCapacityException('Operation cannot fit the in-flight byte budget');
        }
        if (\count($this->admitted) >= $this->limits->maximumAdmittedOperations) {
            throw new PipelineCapacityException('Operation admission count is exhausted');
        }
        if ($credit > $this->limits->maximumAdmittedBytes - $this->admittedBytes) {
            throw new PipelineCapacityException('Operation admission bytes are exhausted');
        }
        /** @var DeferredFuture<\Fabpot\Amp\Sqlite\Worker\DTO\OperationResultDTO> $result */
        $result = new DeferredFuture();
        $entry = new PipelineOperation($result, $mode, $maximumResultBytes, $credit);
        $this->admitted[\spl_object_id($entry)] = $entry;
        $this->admittedBytes += $credit;
        $this->queue[] = new QueuedPipelineOperation($entry, $request);
        $pending = new PendingOperation($entry, $cancellation, fn () => $this->cancelQueued($entry), function () use ($entry): void {
            $entry->consumerFinished = true;
            $this->release($entry);
        });
        $this->wake();

        return $pending;
    }

    public function close(Cancellation $budget): void
    {
        $this->closingConnection = true;
        $subscription = $budget->subscribe(fn () => $this->stopClose(new WorkerFailure('Worker operation lane close was cancelled', previous: new CancelledException())));
        try {
            if ($this->state === PipelineState::Running) {
                $this->state = PipelineState::Draining;
                foreach ($this->queue as $queued) {
                    $this->cancelQueued($queued->operation);
                }
                $this->wake();
            }
            $this->shutdown->getFuture()->await($budget);
            // This method alone owns join. Force-stop paths never join.
            if (!$this->joinStarted) {
                $this->joinStarted = true;
                try {
                    $this->context->join($budget);
                    $this->context->close();
                    $this->joined->complete();
                } catch (\Throwable $failure) {
                    $this->stopClose($failure);
                    $this->joined->error(new WorkerFailure('Worker join failed after graceful close', previous: $failure));
                }
            }
            $this->joined->getFuture()->await($budget);
        } catch (\Throwable $failure) {
            $this->stopClose($failure);
            throw $failure instanceof WorkerFailure ? $failure : new WorkerFailure('Worker operation lane close failed', previous: $failure);
        } finally {
            $budget->unsubscribe($subscription);
        }
    }

    public function abort(): void
    {
        $this->stopClose(new WorkerFailure('Worker operation lane was terminated'));
    }

    private function stopClose(\Throwable $failure): void
    {
        if ($this->state === PipelineState::Stopped) {
            if (!$this->context->isClosed()) {
                $this->context->close();
            }
            return;
        }
        $this->fail($failure);
    }

    public function detach(Cancellation $budget): void
    {
        $subscription = $budget->subscribe(fn () => $this->fail(new WorkerFailure('Operation lane drain was cancelled')));
        try {
            if ($this->state === PipelineState::Running) {
                $this->state = PipelineState::Draining;
                foreach ($this->queue as $queued) {
                    $this->cancelQueued($queued->operation);
                }
                $this->wake();
            }
            $this->shutdown->getFuture()->await($budget);
        } finally {
            $budget->unsubscribe($subscription);
        }
    }

    public function isBusy(): bool
    {
        return $this->flightCount !== 0 || $this->queue !== [];
    }

    public function getLastUsedAt(): int
    {
        return $this->lastUsedAt;
    }

    private function wake(): void
    {
        if (!$this->signal->isComplete()) {
            $this->signal->complete();
        }
    }

    private function write(): void
    {
        try {
            while ($this->state === PipelineState::Running || $this->state === PipelineState::Draining) {
                while ($this->queue !== [] && $this->flightCount < $this->limits->maximumInFlightOperations) {
                    $queued = $this->queue[0];
                    $entry = $queued->operation;
                    if ($entry->creditBytes > $this->limits->maximumInFlightBytes - $this->flightBytes) {
                        break;
                    }
                    \array_shift($this->queue);
                    $id = $this->nextId++;
                    $entry->state = PipelineOperationState::Dispatched;
                    $entry->dispatched = true;
                    $this->flight[$id] = $entry;
                    ++$this->flightCount;
                    $this->flightBytes += $entry->creditBytes;
                    $this->wakeReader();
                    try {
                        $this->context->send(['id' => $id, ...$queued->request]);
                    } finally {
                        unset($queued);
                        $entry->writerReleased = true;
                        $this->release($entry);
                        $this->wake();
                    }
                }
                if ($this->state === PipelineState::Draining && $this->flightCount === 0) {
                    if (!$this->closingConnection) {
                        $this->state = PipelineState::Stopped;
                        ($this->releaseTransport)($this->nextId);
                        $this->wakeReader();
                        $this->shutdown->complete();
                        return;
                    }
                    $this->state = PipelineState::CloseSent;
                    $id = $this->nextId++;
                    $this->wakeReader();
                    $this->context->send(['id' => $id, 'operation' => 'close']);
                    return;
                }
                $signal = $this->signal;
                if ($signal->isComplete()) {
                    $this->signal = new DeferredFuture();
                    continue;
                }
                $signal->getFuture()->await();
                $this->signal = new DeferredFuture();
            }
        } catch (\Throwable $failure) {
            $this->fail($failure);
        }
    }

    private function read(): void
    {
        try {
            while ($this->state !== PipelineState::Failed && $this->state !== PipelineState::Stopped) {
                if ($this->flight === [] && $this->state !== PipelineState::CloseSent) {
                    $this->readerSignal->getFuture()->await();
                    $this->readerSignal = new DeferredFuture();
                    continue;
                }
                $response = $this->context->receive($this->readerCancellation->getCancellation());
                if ($this->state === PipelineState::CloseSent && $this->flight === []) {
                    if (WorkerResponse::unwrap($response, $this->nextId - 1, '') !== null) {
                        throw new WorkerFailure('Worker close reply must be null');
                    }
                    $this->state = PipelineState::Stopped;
                    $this->shutdown->complete();
                    return;
                }
                $id = \array_key_first($this->flight);
                if ($id === null) {
                    throw new WorkerFailure('Unexpected worker operation reply');
                }
                $entry = $this->flight[$id];
                unset($this->flight[$id]);
                $replyBudget = \is_array($response) && \array_key_exists('value', $response)
                    ? $entry->maximumResultBytes + PipelineLimitsDTO::CHANNEL_OVERHEAD_BYTES
                    : $this->limits->maximumErrorReplyBytes;
                if (\strlen(\serialize($response)) > $replyBudget) {
                    throw new WorkerFailure('Worker operation reply exceeds its declared byte budget');
                }
                try {
                    $value = WorkerResponse::unwrap($response, $id, '');
                    $result = WorkerResponse::operation($value, $entry->maximumResultBytes, $entry->mode);
                    $entry->result->complete($result);
                } catch (UnusableOperationException|WorkerFailure $failure) {
                    $this->fail($failure);
                    return;
                } catch (\Fabpot\Amp\Sqlite\SqliteQueryError|\Fabpot\Amp\Sqlite\SqliteException $failure) {
                    $entry->result->error($failure);
                }
                $this->lastUsedAt = \time();
                $entry->state = PipelineOperationState::Finished;
                $this->release($entry);
                $this->wake();
            }
        } catch (\Throwable $failure) {
            $this->fail($failure);
        }
    }

    private function cancelQueued(PipelineOperation $entry): void
    {
        if ($entry->state !== PipelineOperationState::Queued) {
            return;
        }
        $this->queue = \array_values(\array_filter($this->queue, static fn (QueuedPipelineOperation $queued): bool => $queued->operation !== $entry));
        $entry->state = PipelineOperationState::Finished;
        $entry->writerReleased = true;
        if (!$entry->result->isComplete()) {
            $entry->result->error(new CancelledException());
        }
        $this->release($entry);
        $this->wake();
    }

    private function wakeReader(): void
    {
        if (!$this->readerSignal->isComplete()) {
            $this->readerSignal->complete();
        }
    }

    private function release(PipelineOperation $entry): void
    {
        if ($entry->dispatched && $entry->result->isComplete() && $entry->writerReleased && !$entry->flightReleased) {
            $entry->flightReleased = true;
            --$this->flightCount;
            $this->flightBytes -= $entry->creditBytes;
        }
        if ($entry->result->isComplete() && $entry->writerReleased && $entry->consumerFinished && !$entry->admissionReleased) {
            $entry->admissionReleased = true;
            unset($this->admitted[\spl_object_id($entry)]);
            $this->admittedBytes -= $entry->creditBytes;
        }
    }

    private function fail(\Throwable $failure): void
    {
        if ($this->state === PipelineState::Failed || $this->state === PipelineState::Stopped) {
            return;
        }
        $this->state = PipelineState::Failed;
        ($this->onFailure)($failure);
        $this->readerCancellation->cancel();
        $this->wakeReader();
        $this->context->close();
        foreach ($this->queue as $queued) {
            $queued->operation->writerReleased = true;
        }
        $this->queue = [];
        foreach ($this->admitted as $entry) {
            if (!$entry->result->isComplete()) {
                $entry->result->error($failure instanceof \Fabpot\Amp\Sqlite\SqliteQueryError ? $failure : new \Fabpot\Amp\Sqlite\SqliteConnectionException($failure->getMessage(), previous: $failure));
            }
            $entry->state = PipelineOperationState::Finished;
            $this->release($entry);
        }
        $this->wake();
        if (!$this->shutdown->isComplete()) {
            $this->shutdown->error(new WorkerFailure($failure->getMessage(), previous: $failure));
        }
    }
}
