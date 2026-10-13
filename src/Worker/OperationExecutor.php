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

use Amp\ForbidCloning;
use Amp\ForbidSerialization;
use Amp\Sync\Lock;
use Fabpot\Amp\Sqlite\Internal\OperationValues;
use Fabpot\Amp\Sqlite\SqliteConnectionException;
use Fabpot\Amp\Sqlite\Worker\DTO\OperationResultDTO;

/** Parent-side serial executor with exclusive connection ownership. Close releases that ownership. */
final class OperationExecutor
{
    use ForbidCloning;
    use ForbidSerialization;

    private bool $closed = false;
    private int $pendingOperations = 0;
    private bool $released = false;

    /**
     * @param array<string, class-string<Operation>> $operations
     * @param \Closure(string, array<array-key, mixed>, Mode): OperationResultDTO $request
     * @internal Constructed by a connection after acquiring its exclusive lease.
     */
    public function __construct(
        private readonly Lock $ownership,
        private readonly array $operations,
        private readonly \Closure $request,
    ) {
    }

    public function __destruct()
    {
        $this->close();
    }

    /**
     * @param array<array-key, mixed> $parameters
     */
    public function run(string $operation, array $parameters, Mode $mode): OperationResultDTO
    {
        if (!isset($this->operations[$operation])) {
            throw new \InvalidArgumentException('Worker operation is not registered');
        }
        OperationValues::validate($parameters);
        if ($this->closed) {
            throw new SqliteConnectionException('The worker operation executor is closed');
        }
        ++$this->pendingOperations;
        try {
            return ($this->request)($operation, $parameters, $mode);
        } finally {
            --$this->pendingOperations;
            if ($this->closed && $this->pendingOperations === 0) {
                $this->release();
            }
        }
    }

    public function close(): void
    {
        $this->closed = true;
        if ($this->pendingOperations === 0) {
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

    /** @internal Even a closing executor owns its lease until its active request finishes. */
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
