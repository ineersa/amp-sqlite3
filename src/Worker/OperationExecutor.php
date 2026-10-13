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
use Amp\ForbidCloning;
use Amp\ForbidSerialization;
use Amp\NullCancellation;
use Amp\Sync\Lock;
use Fabpot\Amp\Sqlite\Internal\OperationValues;
use Fabpot\Amp\Sqlite\SqliteConnectionException;
use Fabpot\Amp\Sqlite\Worker\DTO\OperationResultDTO;

/** Exclusive connection owner; requests execute through one bounded operation lane. */
final class OperationExecutor
{
    use ForbidCloning;
    use ForbidSerialization;

    private bool $closed = false;
    private bool $released = false;

    /**
     * @param array<string, class-string<Operation>> $operations
     * @param \Closure(string, array<array-key, mixed>, Mode, int, Cancellation): PendingOperation $request
     * @param \Closure(): void $detach
     * @param \Closure(): void $abort Synchronous cleanup only; used during destruction.
     */
    public function __construct(private readonly Lock $ownership, private readonly array $operations, private readonly \Closure $request, private readonly \Closure $detach, private readonly int $maximumResultBytes, private readonly \Closure $abort)
    {
    }

    public function __destruct()
    {
        if ($this->closed) {
            return;
        }
        $this->closed = true;
        try {
            ($this->abort)();
        } catch (\Throwable) {
            // Destruction must not suspend or propagate a shutdown failure.
        } finally {
            $this->release();
        }
    }

    /** Null budgets select the executor's declared maximum; null cancellation means no caller cancellation.
     * @param array<array-key, mixed> $parameters
     */
    public function run(string $operation, array $parameters, Mode $mode, ?int $maximumResultBytes = null, ?Cancellation $cancellation = null): OperationResultDTO
    {
        return $this->submit($operation, $parameters, $mode, $maximumResultBytes, $cancellation)->await();
    }

    /** Explicit handles retain admission until await/discard, including completed replies.
     * @param array<array-key, mixed> $parameters
     */
    public function submit(string $operation, array $parameters, Mode $mode, ?int $maximumResultBytes = null, ?Cancellation $cancellation = null): PendingOperation
    {
        if ($this->closed) {
            throw new SqliteConnectionException('The worker operation executor is closed');
        }
        if (!isset($this->operations[$operation])) {
            throw new \InvalidArgumentException('Worker operation is not registered');
        }
        OperationValues::validate($parameters);
        $bytes = $maximumResultBytes ?? $this->maximumResultBytes;
        if ($bytes < 1) {
            throw new \InvalidArgumentException('Result budget must be positive');
        }
        if ($bytes > $this->maximumResultBytes) {
            throw new \InvalidArgumentException('Result budget exceeds the executor maximum');
        }

        return ($this->request)($operation, $parameters, $mode, $bytes, $cancellation ?? new NullCancellation());
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }
        $this->closed = true;
        try {
            ($this->detach)();
        } finally {
            $this->release();
        }
    }

    public function isClosed(): bool
    {
        return $this->closed;
    }

    /** @internal Connection shutdown must release ownership without awaiting an active request. */
    public function closeOnConnectionClose(): void
    {
        $this->closed = true;
        $this->release();
    }

    public function ownsConnection(): bool
    {
        return !$this->released;
    }

    private function release(): void
    {
        if (!$this->released) {
            $this->released = true;
            $this->ownership->release();
        }
    }
}
