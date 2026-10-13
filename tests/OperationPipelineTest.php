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

use Amp\Cancellation;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\NullCancellation;
use Amp\Parallel\Context\Context;
use Amp\Parallel\Context\ContextException;
use Fabpot\Amp\Sqlite\Internal\OperationPipeline;
use Fabpot\Amp\Sqlite\Internal\WorkerFailure;
use Fabpot\Amp\Sqlite\SqliteConnectionException;
use Fabpot\Amp\Sqlite\Worker\DTO\PipelineLimitsDTO;
use Fabpot\Amp\Sqlite\Worker\Exception\PipelineCapacityException;
use Fabpot\Amp\Sqlite\Worker\Mode;
use Fabpot\Amp\Sqlite\Worker\PendingOperation;
use PHPUnit\Framework\TestCase;

use function Amp\async;

final class OperationPipelineTest extends TestCase
{
    private const RESULT_BYTES = 1024;
    private const LARGE_WINDOW = 65536;

    private function lane(PipelinePeer $peer, int $depth, int $admitted = 8, int $bytes = self::LARGE_WINDOW): OperationPipeline
    {
        return new OperationPipeline($peer, new PipelineLimitsDTO($depth, $bytes, $admitted, self::LARGE_WINDOW, 512), 1, static function (int $nextId): void {
        }, static function (\Throwable $failure): void {
        });
    }

    private function submit(OperationPipeline $lane, Cancellation $cancellation = new NullCancellation()): PendingOperation
    {
        return $lane->submit(['operation' => 'executeWorkerOperation'], Mode::Transaction, self::RESULT_BYTES, $cancellation);
    }

    public function testSecondDispatchPrecedesFirstReplyAndDepthBoundsThird(): void
    {
        $peer = new PipelinePeer();
        $lane = $this->lane($peer, 2);
        try {
            $first = $this->submit($lane);
            $second = $this->submit($lane);
            $third = $this->submit($lane);
            $peer->awaitRequests(2);
            self::assertCount(2, $peer->requests);
            self::assertFalse($first->isComplete());
            self::assertFalse($second->isComplete());
            $peer->reply(1);
            self::assertSame(['id' => 1], $first->await()->data);
            $peer->awaitRequests(3);
            self::assertSame([1, 2, 3], \array_column($peer->requests, 'id'));
            $peer->reply(2);
            $peer->reply(3);
            $second->await();
            $third->await();
        } finally {
            $lane->close(new NullCancellation());
        }
        self::assertSame(1, $peer->joins);
        self::assertSame(0, $peer->activeReads);
    }

    public function testQueuedCancellationDoesNotConsumeAnIdAndDispatchedCancellationDrains(): void
    {
        $peer = new PipelinePeer();
        $lane = $this->lane($peer, 1);
        $dispatched = new DeferredCancellation();
        $queued = new DeferredCancellation();
        $first = $this->submit($lane, $dispatched->getCancellation());
        $peer->awaitRequests(1);
        $second = $this->submit($lane, $queued->getCancellation());
        $third = $this->submit($lane);
        $queued->cancel();
        try {
            $second->getFuture()->await();
            self::fail('Undispatched cancellation must fail completion observation');
        } catch (\Amp\CancelledException) {
            self::assertCount(1, $peer->requests);
        } finally {
            $second->discard();
        }
        $dispatched->cancel();
        $first->discard();
        self::assertCount(1, $peer->requests);
        $peer->reply(1);
        $peer->awaitRequests(2);
        self::assertSame([1, 2], \array_column($peer->requests, 'id'));
        $peer->reply(2);
        self::assertSame(['id' => 2], $third->await()->data);
        $lane->close(new NullCancellation());
    }

    public function testRetainedResultsHoldAdmissionUntilDiscarded(): void
    {
        $peer = new PipelinePeer();
        $lane = $this->lane($peer, 2, 2);
        $first = $this->submit($lane);
        $second = $this->submit($lane);
        $peer->awaitRequests(2);
        $peer->reply(1);
        $peer->awaitReads(2);
        self::assertTrue($first->isComplete());
        try {
            $this->submit($lane);
            self::fail('Admission should be full');
        } catch (PipelineCapacityException) {
            self::assertCount(2, $peer->requests);
        }
        $first->discard();
        $third = $this->submit($lane);
        $peer->awaitRequests(3);
        $peer->reply(2);
        $peer->reply(3);
        $second->await();
        $third->await();
        $lane->close(new NullCancellation());
    }

    public function testReceivedSuccessSurvivesLaterWriterFailureAndWriterStillHoldsAdmission(): void
    {
        $peer = new PipelinePeer();
        $peer->pauseSend(1);
        $lane = $this->lane($peer, 1, 1);
        $first = $this->submit($lane);
        $peer->awaitRequests(1);
        $peer->reply(1);
        self::assertSame(['id' => 1], $first->await()->data);
        try {
            $this->submit($lane);
            self::fail('The suspended writer must retain admission');
        } catch (PipelineCapacityException) {
            self::assertCount(1, $peer->requests);
        }
        $peer->close();
        $peer->awaitSendStopped();
        self::assertSame(['id' => 1], $first->await()->data);
    }

    public function testReplyBudgetAndInFlightBytesBoundDispatch(): void
    {
        $peer = new PipelinePeer();
        $request = ['operation' => 'executeWorkerOperation'];
        $credit = \strlen(\serialize($request)) + 512 + self::RESULT_BYTES + 512;
        $lane = $this->lane($peer, 4, 8, $credit);
        $first = $this->submit($lane);
        $second = $this->submit($lane);
        $peer->awaitRequests(1);
        self::assertCount(1, $peer->requests);
        $peer->reply(1);
        $first->await();
        $peer->awaitRequests(2);
        $peer->reply(2);
        $second->await();
        $lane->close(new NullCancellation());
    }

    public function testOutOfOrderReplyFailsAllUnknownOperations(): void
    {
        $peer = new PipelinePeer();
        $lane = $this->lane($peer, 2);
        $first = $this->submit($lane);
        $second = $this->submit($lane);
        $peer->awaitRequests(2);
        $peer->reply(2);
        foreach ([$first, $second] as $pending) {
            try {
                $pending->await();
                self::fail('Out-of-order reply must fail the lane');
            } catch (SqliteConnectionException) {
                self::assertTrue($peer->isClosed());
            }
        }
        self::assertSame(0, $peer->joins);
    }

    public function testWithheldCloseReplyRemainsBoundedAfterDispatch(): void
    {
        $this->assertWithheldCloseIsCancelled();
    }

    public function testSameCoordinatorExecutesNativeTransactionsSequentiallyAtBothDepths(): void
    {
        foreach ([1, 4] as $depth) {
            $connection = (new \Fabpot\Amp\Sqlite\SqliteConnector())->connect((new \Fabpot\Amp\Sqlite\SqliteConfig(':memory:'))->withWorkerOperations(['counter' => CounterWorkerOperation::class]));
            $connection->execute('CREATE TABLE counter (value INTEGER NOT NULL)')->close();
            $connection->execute('INSERT INTO counter VALUES (10)')->close();
            $executor = $connection->createWorkerOperationExecutor(self::RESULT_BYTES, 1, new PipelineLimitsDTO($depth, self::LARGE_WINDOW, 8, self::LARGE_WINDOW, 512));
            try {
                $pending = [];
                for ($i = 0; $i < 4; ++$i) {
                    $pending[] = $executor->submit('counter', ['decrement' => 1, 'reject' => false], Mode::Transaction);
                }
                $values = [];
                foreach ($pending as $operation) {
                    $values[] = $operation->await()->data['value'];
                }
                self::assertSame([9, 8, 7, 6], $values);
                $executor->close();
                $result = $connection->query('SELECT value FROM counter');
                self::assertSame(['value' => 6], $result->fetchRow());
                $result->close();
            } finally {
                $connection->close();
            }
        }
    }

    public function testAdmissionBytesIncludeQueuedWorkAndRetainedReplies(): void
    {
        $peer = new PipelinePeer();
        $credit = \strlen(\serialize(['operation' => 'executeWorkerOperation'])) + 512 + self::RESULT_BYTES + 512;
        $lane = new OperationPipeline($peer, new PipelineLimitsDTO(1, $credit, 8, 2 * $credit, 512), 1, static function (int $id): void {
        }, static function (\Throwable $failure): void {
        });
        $first = $this->submit($lane);
        $second = $this->submit($lane);
        $peer->awaitRequests(1);
        try {
            $this->submit($lane);
            self::fail('Queued request/reply credit must count against total bytes');
        } catch (PipelineCapacityException) {
            self::assertCount(1, $peer->requests);
        }
        $peer->reply(1);
        $first->await();
        $peer->awaitRequests(2);
        $peer->reply(2);
        $second->await();
        $lane->close(new NullCancellation());
    }

    public function testKnownSuccessSurvivesMalformedLaterReply(): void
    {
        $peer = new PipelinePeer();
        $lane = $this->lane($peer, 2);
        $first = $this->submit($lane);
        $second = $this->submit($lane);
        $peer->awaitRequests(2);
        $peer->reply(1);
        $peer->awaitReads(2);
        $peer->malformedReply(2);
        try {
            $second->await();
            self::fail('Malformed operation result must fail the lane');
        } catch (SqliteConnectionException) {
            self::assertTrue($peer->isClosed());
        }
        self::assertSame(['id' => 1], $first->await()->data);
    }

    public function testConcurrentGracefulCloseHasExactlyOneJoinOwner(): void
    {
        $peer = new PipelinePeer();
        $lane = $this->lane($peer, 1);
        $first = async(static fn () => $lane->close(new NullCancellation()));
        $second = async(static fn () => $lane->close(new NullCancellation()));
        $first->await();
        $second->await();
        self::assertSame(1, $peer->joins);
        self::assertCount(1, $peer->requests);
    }

    public function testConcurrentCloseSharesJoinFailureWithoutFailingStoppedLane(): void
    {
        $peer = new PipelinePeer();
        $peer->failJoin = true;
        $lane = $this->lane($peer, 1);
        $first = async(static fn () => $lane->close(new NullCancellation()));
        $second = async(static fn () => $lane->close(new NullCancellation()));
        foreach ([$first, $second] as $closing) {
            try {
                $closing->await();
                self::fail('Join error must be normalized');
            } catch (WorkerFailure $failure) {
                self::assertInstanceOf(\RuntimeException::class, $failure->getPrevious());
                self::assertSame('scripted join failure', $failure->getPrevious()->getMessage());
            }
        }
        self::assertSame(1, $peer->joins);
        self::assertTrue($peer->isClosed());
        self::assertSame(\Fabpot\Amp\Sqlite\Internal\PipelineState::Stopped, (new \ReflectionProperty($lane, 'state'))->getValue($lane));
    }

    public function testDroppingExecutorAbortsWithoutSuspensionAndReleasesOwnership(): void
    {
        $peer = new PipelinePeer();
        $lane = $this->lane($peer, 1);
        $mutex = new \Amp\Sync\LocalMutex();
        $executor = new \Fabpot\Amp\Sqlite\Worker\OperationExecutor($mutex->acquire(), ['counter' => CounterWorkerOperation::class], static fn (string $name, array $parameters, Mode $mode, int $bytes, Cancellation $cancellation): PendingOperation => $lane->submit(['operation' => 'executeWorkerOperation'], $mode, $bytes, $cancellation), static function (): void {
            self::fail('Destructor must not drain');
        }, self::RESULT_BYTES, $lane->abort(...));
        $pending = $executor->submit('counter', [], Mode::Transaction);
        $peer->awaitRequests(1);
        $weak = \WeakReference::create($executor);
        unset($executor);
        self::assertNull($weak->get());
        self::assertTrue($peer->isClosed());
        self::assertSame(0, $peer->joins);
        $lock = $mutex->acquire();
        $lock->release();
        try {
            $pending->getFuture()->await();
            self::fail('Pending work must fail after synchronous abort');
        } catch (SqliteConnectionException) {
            $pending->discard();
        }
    }

    public function testCompletionObserverDrainsConfirmedResultAfterDispatchCancellation(): void
    {
        $peer = new PipelinePeer();
        $lane = $this->lane($peer, 1);
        $cancellation = new DeferredCancellation();
        $pending = $this->submit($lane, $cancellation->getCancellation());
        $completion = $pending->getFuture();
        $peer->awaitRequests(1);
        $cancellation->cancel();
        self::assertFalse($completion->isComplete());
        $peer->reply(1);
        try {
            self::assertSame(['id' => 1], $completion->await()->data);
        } finally {
            $pending->discard();
        }
        $lane->close(new NullCancellation());
    }

    public function testErrorEnvelopeBudgetIsIndependentOfLargeValueBudget(): void
    {
        foreach ([511, 512, 513] as $bytes) {
            $peer = new PipelinePeer();
            $lane = $this->lane($peer, 1);
            $pending = $this->submit($lane);
            $peer->awaitRequests(1);
            $response = $this->sizedError($bytes);
            self::assertSame($bytes, \strlen(\serialize($response)));
            $peer->rawReply($response);
            try {
                $pending->await();
                self::fail('Error reply must not return a success');
            } catch (\Fabpot\Amp\Sqlite\SqliteQueryError $failure) {
                self::assertLessThanOrEqual(512, $bytes);
                self::assertSame(13, $failure->getResultCode());
                self::assertSame(13, $failure->getExtendedResultCode());
                self::assertSame('INSERT INTO counter VALUES (?)', $failure->getQuery());
                self::assertFalse($peer->isClosed());
                $lane->close(new NullCancellation());
            } catch (SqliteConnectionException) {
                self::assertSame(513, $bytes);
                self::assertTrue($peer->isClosed());
            }
        }
    }

    /** @return array<string, mixed> */
    private function sizedError(int $bytes): array
    {
        for ($padding = 0; $padding < $bytes; ++$padding) {
            $reply = ['id' => 1, 'query_error' => ['message' => \str_repeat('x', $padding), 'code' => 13, 'extended_code' => 13, 'query' => 'INSERT INTO counter VALUES (?)']];
            if (\strlen(\serialize($reply)) === $bytes) {
                return $reply;
            }
        }
        throw new \LogicException('Could not construct exact error envelope size');
    }

    private function assertWithheldCloseIsCancelled(): void
    {
        $peer = new PipelinePeer();
        $peer->withholdClose = true;
        $lane = $this->lane($peer, 1);
        $budget = new DeferredCancellation();
        $closing = async(static fn () => $lane->close($budget->getCancellation()));
        $peer->awaitRequests(1);
        self::assertSame('close', $peer->requests[0]['operation']);
        $budget->cancel();
        try {
            $closing->await();
            self::fail('Close should fail when cancelled');
        } catch (\Throwable $failure) {
            self::assertTrue($failure instanceof WorkerFailure || $failure instanceof \Amp\CancelledException, \get_debug_type($failure) . ': ' . $failure->getMessage());
            self::assertTrue($peer->isClosed());
            self::assertSame(0, $peer->joins);
        }
        $peer->awaitReadStopped();
        self::assertSame(0, $peer->activeReads);
    }
}

/** @implements Context<null, mixed, array<string, mixed>> */
final class PipelinePeer implements Context
{
    /** @var list<array<string, mixed>> */
    public array $requests = [];
    /** @var list<array<string, mixed>> */
    private array $replies = [];
    /** @var array<int, DeferredFuture<null>> */
    private array $sendGates = [];
    /** @var list<\Closure(): void> */
    private array $callbacks = [];
    private bool $closed = false;
    public bool $withholdClose = false;
    public bool $failJoin = false;
    public int $joins = 0;
    public int $activeReads = 0;
    private int $reads = 0;
    private DeferredFuture $readSignal;
    private DeferredFuture $requestSignal;
    private DeferredFuture $replySignal;
    private readonly DeferredFuture $readStopped;
    private readonly DeferredFuture $sendStopped;

    public function __construct()
    {
        $this->requestSignal = new DeferredFuture();
        $this->readSignal = new DeferredFuture();
        $this->replySignal = new DeferredFuture();
        $this->replySignal->getFuture()->ignore();
        $this->readStopped = new DeferredFuture();
        $this->sendStopped = new DeferredFuture();
    }

    public function pauseSend(int $number): void
    {
        $this->sendGates[$number] = new DeferredFuture();
        $this->sendGates[$number]->getFuture()->ignore();
    }

    public function send(mixed $data): void
    {
        $this->requests[] = $data;
        if (!$this->requestSignal->isComplete()) {
            $this->requestSignal->complete();
        }
        $number = \count($this->requests);
        if (isset($this->sendGates[$number])) {
            try {
                $this->sendGates[$number]->getFuture()->await();
            } finally {
                $this->sendStopped->complete();
            }
        }
        if ($data['operation'] === 'close' && !$this->withholdClose) {
            $this->push(['id' => $data['id'], 'value' => null]);
        }
    }

    public function receive(?Cancellation $cancellation = null): mixed
    {
        ++$this->reads;
        if (!$this->readSignal->isComplete()) {
            $this->readSignal->complete();
        }
        ++$this->activeReads;
        try {
            while ($this->replies === []) {
                if ($this->closed) {
                    throw new ContextException('Peer closed');
                }
                $this->replySignal->getFuture()->await($cancellation);
                $this->replySignal = new DeferredFuture();
                $this->replySignal->getFuture()->ignore();
            }
            return \array_shift($this->replies);
        } finally {
            --$this->activeReads;
            if ($this->closed && !$this->readStopped->isComplete()) {
                $this->readStopped->complete();
            }
        }
    }

    public function reply(int $id): void
    {
        $this->push(['id' => $id, 'value' => ['outcome' => 'commit', 'data' => ['id' => $id]]]);
    }

    public function malformedReply(int $id): void
    {
        $this->push(['id' => $id, 'value' => ['outcome' => 'commit', 'data' => 'not-an-array']]);
    }

    /** @param array<string, mixed> $reply */
    public function rawReply(array $reply): void
    {
        $this->push($reply);
    }

    /** @param array<string, mixed> $reply */
    private function push(array $reply): void
    {
        $this->replies[] = $reply;
        if (!$this->replySignal->isComplete()) {
            $this->replySignal->complete();
        }
    }

    public function awaitRequests(int $count): void
    {
        while (\count($this->requests) < $count) {
            $this->requestSignal->getFuture()->await();
            $this->requestSignal = new DeferredFuture();
        }
    }

    public function awaitReadStopped(): void
    {
        $this->readStopped->getFuture()->await();
    }

    public function awaitReads(int $count): void
    {
        while ($this->reads < $count) {
            $this->readSignal->getFuture()->await();
            $this->readSignal = new DeferredFuture();
        }
    }

    public function awaitSendStopped(): void
    {
        $this->sendStopped->getFuture()->await();
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }
        $this->closed = true;
        foreach ($this->sendGates as $gate) {
            if (!$gate->isComplete()) {
                $gate->error(new ContextException('Peer closed during send'));
            }
        }
        if (!$this->replySignal->isComplete()) {
            $this->replySignal->error(new ContextException('Peer closed during receive'));
        }
        foreach ($this->callbacks as $callback) {
            $callback();
        }
    }

    public function isClosed(): bool
    {
        return $this->closed;
    }

    public function onClose(\Closure $onClose): void
    {
        $this->callbacks[] = $onClose;
    }

    public function join(?Cancellation $cancellation = null): mixed
    {
        ++$this->joins;
        if ($this->failJoin) {
            throw new \RuntimeException('scripted join failure');
        }
        return null;
    }
}
