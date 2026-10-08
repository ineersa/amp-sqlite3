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

namespace Fabpot\Amp\Sqlite\Test;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Parallel\Context\Context;
use Amp\Parallel\Context\ContextException;
use Amp\Sync\ChannelException;
use Fabpot\Amp\Sqlite\Internal\WorkerChannel;
use Fabpot\Amp\Sqlite\Internal\WorkerFailure;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;
use function Amp\async;

final class WorkerChannelCloseTest extends TestCase
{
    public function testCloseCancelsASuspendedSend(): void
    {
        $context = new ScriptedCloseContext(suspendSend: true);
        $channel = new WorkerChannel($context);
        $budget = new DeferredCancellation();

        $future = async(static fn () => $channel->close($budget->getCancellation()));
        $context->awaitSendSuspended();
        self::assertFalse($future->isComplete());
        self::assertFalse($context->isClosed());

        $budget->cancel();
        $this->awaitFailure($future);

        self::assertTrue($context->isClosed());
        self::assertSame(0, $context->joinCalls);
        self::assertSame(1, $context->closeCalls);
    }

    public function testCloseCancelsAPendingReceive(): void
    {
        $context = new ScriptedCloseContext(suspendReceive: true);
        $channel = new WorkerChannel($context);
        $budget = new DeferredCancellation();

        $future = async(static fn () => $channel->close($budget->getCancellation()));
        $context->awaitReceiveSuspended();
        self::assertFalse($future->isComplete());
        self::assertFalse($context->isClosed());

        $budget->cancel();
        $this->awaitFailure($future);

        self::assertTrue($context->isClosed());
        self::assertSame(0, $context->joinCalls);
        self::assertSame(1, $context->closeCalls);
    }

    public function testCloseCancelsAPendingJoin(): void
    {
        $context = new ScriptedCloseContext(suspendJoin: true);
        $channel = new WorkerChannel($context);
        $budget = new DeferredCancellation();

        $future = async(static fn () => $channel->close($budget->getCancellation()));
        $context->awaitJoinSuspended();
        self::assertFalse($future->isComplete());
        self::assertFalse($context->isClosed());

        $budget->cancel();
        $this->awaitFailure($future);

        self::assertTrue($context->isClosed());
        self::assertSame(1, $context->joinCalls);
        self::assertSame(1, $context->closeCalls);
    }

    public function testOrdinaryCloseJoinsTheChildOnce(): void
    {
        $context = new ScriptedCloseContext();
        $channel = new WorkerChannel($context);

        $channel->close();

        self::assertTrue($context->isClosed());
        self::assertSame(1, $context->joinCalls);
        self::assertSame(1, $context->closeCalls);
        self::assertSame([['id' => 1, 'value' => null]], $context->replies);
    }

    /**
     * @param \Amp\Future<null> $future
     */
    private function awaitFailure(\Amp\Future $future): void
    {
        try {
            $future->await();
            self::fail('Expected a cancelled close to fail the channel');
        } catch (WorkerFailure $failure) {
            self::assertSame('The SQLite child process close was cancelled', $failure->getMessage());
            self::assertInstanceOf(CancelledException::class, $failure->getPrevious());
        }
    }
}

/**
 * @implements Context<null, array<string, mixed>, array<string, mixed>>
 */
final class ScriptedCloseContext implements Context
{
    /** @var list<array<string, mixed>> */
    private array $inbox = [];

    /** @var list<array<string, mixed>> */
    public array $replies = [];

    /** @var list<\Closure(): void> */
    private array $onClose = [];

    private bool $closed = false;
    public int $closeCalls = 0;
    public int $joinCalls = 0;

    /** @var DeferredFuture<null>|null */
    private ?DeferredFuture $sendSuspended = null;

    /** @var DeferredFuture<null>|null */
    private ?DeferredFuture $receiveSuspended = null;

    /** @var DeferredFuture<null>|null */
    private ?DeferredFuture $joinSuspended = null;

    public function __construct(
        private readonly bool $suspendSend = false,
        private readonly bool $suspendReceive = false,
        private readonly bool $suspendJoin = false,
    ) {
    }

    public function receive(?\Amp\Cancellation $cancellation = null): mixed
    {
        if ($this->suspendReceive) {
            $this->receiveSuspended ??= new DeferredFuture();
            if (!$this->receiveSuspended->isComplete()) {
                $this->receiveSuspended->complete(null);
            }

            $pending = new DeferredFuture();
            $id = $cancellation?->subscribe(static function () use ($pending): void {
                if (!$pending->isComplete()) {
                    $pending->error(new CancelledException());
                }
            });

            try {
                return $pending->getFuture()->await();
            } finally {
                if ($id !== null) {
                    $cancellation?->unsubscribe($id);
                }
            }
        }

        if ($this->inbox === []) {
            throw new ChannelException('No queued reply');
        }

        $reply = \array_shift($this->inbox);
        $this->replies[] = $reply;

        return $reply;
    }

    public function send(mixed $data): void
    {
        if ($this->closed) {
            throw new ContextException('The context is closed');
        }
        if (!\is_array($data) || ($data['operation'] ?? null) !== 'close') {
            throw new \LogicException('ScriptedCloseContext only accepts close requests');
        }

        if ($this->suspendSend) {
            $this->sendSuspended ??= new DeferredFuture();
            if (!$this->sendSuspended->isComplete()) {
                $this->sendSuspended->complete(null);
            }

            $pending = new DeferredFuture();
            $this->onClose(static function () use ($pending): void {
                if (!$pending->isComplete()) {
                    $pending->error(new ContextException('The context stopped responding during send'));
                }
            });
            $pending->getFuture()->await();
        }

        if (!$this->suspendReceive) {
            $this->inbox[] = ['id' => $data['id'], 'value' => null];
        }
    }

    public function awaitSendSuspended(): void
    {
        $this->sendSuspended ??= new DeferredFuture();
        $this->sendSuspended->getFuture()->await();
    }

    public function awaitReceiveSuspended(): void
    {
        $this->receiveSuspended ??= new DeferredFuture();
        $this->receiveSuspended->getFuture()->await();
    }

    public function awaitJoinSuspended(): void
    {
        $this->joinSuspended ??= new DeferredFuture();
        $this->joinSuspended->getFuture()->await();
    }

    public function close(): void
    {
        ++$this->closeCalls;
        if ($this->closed) {
            return;
        }

        $this->closed = true;
        foreach ($this->onClose as $callback) {
            EventLoop::queue($callback);
        }
        $this->onClose = [];
    }

    public function isClosed(): bool
    {
        return $this->closed;
    }

    public function onClose(\Closure $onClose): void
    {
        if ($this->closed) {
            EventLoop::queue($onClose);

            return;
        }

        $this->onClose[] = $onClose;
    }

    public function join(?\Amp\Cancellation $cancellation = null): mixed
    {
        ++$this->joinCalls;
        if ($this->suspendJoin) {
            $this->joinSuspended ??= new DeferredFuture();
            if (!$this->joinSuspended->isComplete()) {
                $this->joinSuspended->complete(null);
            }

            $pending = new DeferredFuture();
            $id = $cancellation?->subscribe(static function () use ($pending): void {
                if (!$pending->isComplete()) {
                    $pending->error(new CancelledException());
                }
            });

            try {
                return $pending->getFuture()->await();
            } finally {
                if ($id !== null) {
                    $cancellation?->unsubscribe($id);
                }
            }
        }

        return null;
    }
}
