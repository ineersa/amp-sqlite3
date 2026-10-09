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

use Amp\Sync\LocalMutex;
use Amp\Sync\Lock;
use Amp\TimeoutCancellation;
use Fabpot\Amp\Sqlite\Internal\Result;
use Fabpot\Amp\Sqlite\SqliteConfig;
use Fabpot\Amp\Sqlite\SqliteConnectionPool;
use Fabpot\Amp\Sqlite\SqliteConnector;
use PHPUnit\Framework\TestCase;
use function Amp\async;
use function Amp\delay;

final class ResultCloseTest extends TestCase
{
    public function testClosingFullyBufferedUnconsumedResultSkipsRemoteClose(): void
    {
        $closes = 0;
        $result = $this->bufferedResult($closes, [
            ['value' => 1],
            ['value' => 2],
        ]);

        $result->close();

        self::assertSame(0, $closes);
        self::assertTrue($result->isClosed());
    }

    public function testClosingPartiallyConsumedBufferedResultSkipsRemoteClose(): void
    {
        $closes = 0;
        $result = $this->bufferedResult($closes, [
            ['value' => 1],
            ['value' => 2],
            ['value' => 3],
        ]);

        self::assertSame(['value' => 1], $result->fetchRow());
        $result->close();

        self::assertSame(0, $closes);
        self::assertTrue($result->isClosed());
    }

    public function testDroppingBufferedResultSkipsRemoteClose(): void
    {
        $closes = 0;
        $completed = 0;
        $result = $this->bufferedResult($closes, [
            ['value' => 1],
            ['value' => 2],
        ], onClose: static function () use (&$completed): void {
            ++$completed;
        });

        unset($result);
        self::drainQueuedCallbacks();

        self::assertSame(0, $closes);
        self::assertSame(1, $completed);
    }

    public function testClosingAfterLaterExhaustedBatchSkipsRemoteClose(): void
    {
        $closes = 0;
        $fetches = 0;
        $result = new Result(
            [['value' => 1]],
            null,
            1,
            ['value'],
            null,
            7,
            false,
            static function () use (&$fetches): array {
                ++$fetches;

                return [
                    'rows' => [['value' => 2]],
                    'exhausted' => true,
                ];
            },
            static function () use (&$closes): void {
                ++$closes;
            },
            null,
        );

        self::assertSame(['value' => 1], $result->fetchRow());
        self::assertSame(['value' => 2], $result->fetchRow());
        self::assertNull($result->fetchRow());
        $result->close();

        self::assertSame(1, $fetches);
        self::assertSame(0, $closes);
        self::assertTrue($result->isClosed());
    }

    public function testClosingStreamingResultSendsOneRemoteClose(): void
    {
        $closes = 0;
        $result = new Result(
            [['value' => 1]],
            null,
            1,
            ['value'],
            null,
            9,
            false,
            static function (): array {
                return [
                    'rows' => [['value' => 2]],
                    'exhausted' => false,
                ];
            },
            static function () use (&$closes): void {
                ++$closes;
            },
            null,
        );

        self::assertSame(['value' => 1], $result->fetchRow());
        $result->close();

        self::assertSame(1, $closes);
        self::assertTrue($result->isClosed());
    }

    public function testDroppingStreamingResultSendsOneRemoteClose(): void
    {
        $closes = 0;
        $completed = 0;
        $result = new Result(
            [['value' => 1]],
            null,
            1,
            ['value'],
            null,
            11,
            false,
            null,
            static function () use (&$closes): void {
                ++$closes;
            },
            null,
        );
        $result->onClose(static function () use (&$completed): void {
            ++$completed;
        });

        unset($result);
        self::drainQueuedCallbacks();

        self::assertSame(1, $closes);
        self::assertSame(1, $completed);
    }

    public function testBufferedRowsRemainReadableAfterConnectionCloseWithoutRemoteClose(): void
    {
        $connection = (new SqliteConnector())->connect((new SqliteConfig(':memory:'))->withBatchSize(2));
        $result = $connection->query('SELECT 1 AS value UNION ALL SELECT 2');
        $connection->close();

        self::assertSame([['value' => 1], ['value' => 2]], \iterator_to_array($result));
        self::assertTrue($result->isClosed());
    }

    public function testPoolReleasesAfterClosingExhaustedBufferedResult(): void
    {
        $path = \sys_get_temp_dir().'/amp-sqlite-result-close-'.\bin2hex(\random_bytes(8)).'.sqlite';
        $pool = new SqliteConnectionPool((new SqliteConfig($path))->withBatchSize(2), maxConnections: 1);

        try {
            $result = $pool->query('SELECT 1 AS value UNION ALL SELECT 2');
            $waiting = async(static fn () => $pool->query('SELECT 42 AS answer')->fetchRow());
            delay(0);
            self::assertFalse($waiting->isComplete());

            $result->close();

            self::assertSame(['answer' => 42], $waiting->await());
        } finally {
            $pool->close();
            @\unlink($path);
            @\unlink($path.'-shm');
            @\unlink($path.'-wal');
        }
    }

    public function testLeaseReleasedOnceWhenDroppingExhaustedBufferedResult(): void
    {
        $closes = 0;
        $releases = 0;
        $mutex = new LocalMutex();
        $lease = $mutex->acquire();
        $result = new Result(
            [['value' => 1]],
            null,
            1,
            ['value'],
            null,
            13,
            true,
            null,
            static function () use (&$closes): void {
                ++$closes;
            },
            new Lock(static function () use (&$releases, $lease): void {
                ++$releases;
                $lease->release();
            }),
        );
        $completed = 0;
        $result->onClose(static function () use (&$completed): void {
            ++$completed;
        });

        unset($result);
        self::drainQueuedCallbacks();

        self::assertSame(0, $closes);
        self::assertSame(1, $releases);
        self::assertSame(1, $completed);
    }

    /**
     * @param list<array<string, int>> $rows
     * @param null|\Closure():void $onClose
     */
    private function bufferedResult(int &$closes, array $rows, ?\Closure $onClose = null): Result
    {
        $result = new Result(
            $rows,
            null,
            1,
            ['value'],
            null,
            3,
            true,
            null,
            static function () use (&$closes): void {
                ++$closes;
            },
            null,
        );
        if ($onClose !== null) {
            $result->onClose($onClose);
        }

        return $result;
    }

    private static function drainQueuedCallbacks(): void
    {
        async(static fn () => null)->await(new TimeoutCancellation(5));
    }
}
