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
use Amp\CompositeCancellation;
use Amp\ForbidCloning;
use Amp\ForbidSerialization;
use Amp\Parallel\Context\Context;
use Amp\Sync\LocalMutex;
use Amp\TimeoutCancellation;
use Fabpot\Amp\Sqlite\SqliteException;
use Fabpot\Amp\Sqlite\SqliteQueryError;

/**
 * Sends requests to the SQLite child process one at a time.
 *
 * @internal
 */
final class WorkerChannel
{
    use ForbidCloning;
    use ForbidSerialization;

    /**
     * Upper bound for a graceful child shutdown when the caller does not supply a tighter cancellation.
     *
     * A peer that withholds the close reply, or a join that never observes exit, is force-terminated
     * when this budget expires. Callers may pass a shorter cancellation to close().
     */
    public const float DEFAULT_CLOSE_TIMEOUT_SECONDS = 5.0;

    private readonly LocalMutex $mutex;
    private int $nextRequestId = 1;
    private bool $busy = false;
    private int $lastUsedAt;

    /**
     * @param Context<null, mixed, array<string, mixed>> $context
     */
    public function __construct(
        private readonly Context $context,
    ) {
        $this->mutex = new LocalMutex();
        $this->lastUsedAt = \time();
    }

    /**
     * @param array<string, mixed> $data
     *
     * @throws SqliteQueryError If SQLite rejected the SQL
     * @throws SqliteException  If another SQLite operation failed
     * @throws WorkerFailure    If the child process stopped or broke the protocol
     */
    public function request(string $operation, string $sql, #[\SensitiveParameter] array $data): mixed
    {
        $lock = $this->mutex->acquire();
        $id = $this->nextRequestId++;
        $this->busy = true;

        try {
            $this->context->send(['id' => $id, 'operation' => $operation, ...$data]);
            $response = $this->context->receive();
        } catch (\Throwable $exception) {
            throw new WorkerFailure('The SQLite child process stopped unexpectedly', previous: $exception);
        } finally {
            $this->busy = false;
            $lock->release();
        }

        $this->lastUsedAt = \time();

        return WorkerResponse::unwrap($response, $id, $sql);
    }

    public function isBusy(): bool
    {
        return $this->busy;
    }

    public function isClosed(): bool
    {
        return $this->context->isClosed();
    }

    public function getLastUsedAt(): int
    {
        return $this->lastUsedAt;
    }

    /**
     * Asks the child process to close the database and waits for it to exit.
     *
     * The wait is bounded by {@see self::DEFAULT_CLOSE_TIMEOUT_SECONDS}. Pass a cancellation to
     * tighten that budget. The same budget independently force-terminates a suspended send,
     * receive, or join. Cancellation or any shutdown failure force-terminates the child and
     * leaves process reaping to Amp; this method never joins after force termination.
     *
     * @throws WorkerFailure If the child process did not shut down cleanly
     */
    public function close(?Cancellation $cancellation = null): void
    {
        $budget = self::closeBudget($cancellation);
        $subscription = $budget->subscribe($this->kill(...));
        $lock = null;

        try {
            $lock = $this->mutex->acquire();
            if ($this->context->isClosed()) {
                return;
            }

            $this->context->send(['id' => $this->nextRequestId++, 'operation' => 'close']);
            $this->context->receive($budget);
            $this->context->join($budget);
            $this->context->close();
        } catch (\Throwable $exception) {
            $this->kill();
            if ($exception instanceof CancelledException || $budget->isRequested()) {
                throw new WorkerFailure(
                    'The SQLite child process close was cancelled',
                    previous: $exception instanceof CancelledException
                        ? $exception
                        : new CancelledException(previous: $exception),
                );
            }

            throw new WorkerFailure('The SQLite child process stopped unexpectedly', previous: $exception);
        } finally {
            $budget->unsubscribe($subscription);
            $lock?->release();
        }
    }

    /**
     * Force-terminates the child process without waiting for a close reply or exit result.
     *
     * Amp retains process ownership after the IPC channels close, so this method does not join.
     */
    public function kill(): void
    {
        if ($this->context->isClosed()) {
            return;
        }

        $this->context->close();
    }

    private static function closeBudget(?Cancellation $cancellation): Cancellation
    {
        $timeout = new TimeoutCancellation(self::DEFAULT_CLOSE_TIMEOUT_SECONDS);
        if ($cancellation === null) {
            return $timeout;
        }

        return new CompositeCancellation($timeout, $cancellation);
    }
}
