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

use Amp\DeferredCancellation;
use Amp\TimeoutCancellation;
use Fabpot\Amp\Sqlite\SqliteConfig;
use Fabpot\Amp\Sqlite\SqliteConnectionException;
use Fabpot\Amp\Sqlite\SqliteConnector;
use Fabpot\Amp\Sqlite\SqliteQueryError;
use Fabpot\Amp\Sqlite\SqliteTransactionError;
use Fabpot\Amp\Sqlite\SqliteWorkerOperationConnection;
use Fabpot\Amp\Sqlite\Worker\Mode;
use Fabpot\Amp\Sqlite\Worker\Outcome;
use PHPUnit\Framework\TestCase;
use function Amp\async;
use function Amp\Socket\listen;

final class WorkerOperationTest extends TestCase
{
    private const INITIAL_VALUE = 10;
    private const RESULT_BYTES = 16_384;
    private const RESULT_ROWS = 16;
    private const SQLITE_PRIMARY_ERROR = 1;
    private const SQLITE_PRIMARY_CONSTRAINT = 19;
    private const SQLITE_EXTENDED_FOREIGN_KEY = 787;

    private function connect(string $path): SqliteWorkerOperationConnection
    {
        $connection = (new SqliteConnector())->connect((new SqliteConfig($path))->withBatchSize(1)->withWorkerOperations([
            'counter' => CounterWorkerOperation::class,
            'sql' => SqlWorkerOperation::class,
            'pause' => PausedWorkerOperation::class,
        ]));
        $connection->query('CREATE TABLE counter (value INTEGER NOT NULL)')->close();
        $connection->execute('INSERT INTO counter VALUES (?)', [self::INITIAL_VALUE])->close();

        return $connection;
    }

    public function testConditionalCounterCommitAndExplicitRollback(): void
    {
        $connection = $this->connect(':memory:');
        $executor = $connection->createWorkerOperationExecutor(self::RESULT_BYTES, self::RESULT_ROWS);
        try {
            $result = $executor->run('counter', ['decrement' => 3, 'reject' => false], Mode::Transaction);
            self::assertSame(Outcome::Commit, $result->outcome);
            self::assertSame(['value' => 7], $result->data);
            $result = $executor->run('counter', ['decrement' => 9, 'reject' => false], Mode::Transaction);
            self::assertSame(Outcome::Rollback, $result->outcome);
            $executor->close();
            self::assertSame(['value' => 7], $connection->query('SELECT value FROM counter')->fetchRow());
        } finally {
            $executor->close();
            $connection->close();
        }
    }

    public function testValidationAfterUpdateRollsBackWithNullNativeCodes(): void
    {
        $connection = $this->connect(':memory:');
        $executor = $connection->createWorkerOperationExecutor(self::RESULT_BYTES, self::RESULT_ROWS);
        try {
            try {
                $executor->run('counter', ['decrement' => 3, 'reject' => true], Mode::Transaction);
                self::fail('Expected application validation failure');
            } catch (SqliteQueryError $error) {
                self::assertSame('Counter validation failed after update', $error->getMessage());
                self::assertNull($error->getResultCode());
                self::assertNull($error->getExtendedResultCode());
                self::assertSame('', $error->getQuery());
            }
            $result = $executor->run('sql', ['sql' => 'SELECT value FROM counter', 'bindings' => [], 'outcome' => Outcome::ReadOnly->value], Mode::ReadOnly);
            self::assertSame([['value' => self::INITIAL_VALUE]], $result->data['rows']);
        } finally {
            $executor->close();
            $connection->close();
        }
    }

    public function testNativeSqlErrorRetainsQueryAndCodes(): void
    {
        $connection = $this->connect(':memory:');
        $executor = $connection->createWorkerOperationExecutor(self::RESULT_BYTES, self::RESULT_ROWS);
        try {
            $sql = 'INSERT INTO missing VALUES (1)';
            try {
                $executor->run('sql', ['sql' => $sql, 'bindings' => [], 'outcome' => Outcome::Commit->value], Mode::Transaction);
                self::fail('Expected native SQL failure');
            } catch (SqliteQueryError $error) {
                self::assertSame($sql, $error->getQuery());
                self::assertSame(self::SQLITE_PRIMARY_ERROR, $error->getResultCode());
                self::assertSame(self::SQLITE_PRIMARY_ERROR, $error->getExtendedResultCode());
            }
        } finally {
            $executor->close();
            $connection->close();
        }
    }

    public function testDeferredForeignKeyFailureIsAttributedToCommit(): void
    {
        $connection = $this->connect(':memory:');
        $connection->query('CREATE TABLE parent (id INTEGER PRIMARY KEY)')->close();
        $connection->query('CREATE TABLE child (parent_id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)')->close();
        $executor = $connection->createWorkerOperationExecutor(self::RESULT_BYTES, self::RESULT_ROWS);
        try {
            try {
                $executor->run('sql', ['sql' => 'INSERT INTO child VALUES (999)', 'bindings' => [], 'outcome' => Outcome::Commit->value], Mode::Transaction);
                self::fail('Expected COMMIT failure');
            } catch (SqliteQueryError $error) {
                self::assertSame('COMMIT', $error->getQuery());
                self::assertSame(self::SQLITE_PRIMARY_CONSTRAINT, $error->getResultCode());
                self::assertSame(self::SQLITE_EXTENDED_FOREIGN_KEY, $error->getExtendedResultCode());
            }
            $executor->close();
            self::assertSame(['count' => 0], $connection->query('SELECT count(*) AS count FROM child')->fetchRow());
        } finally {
            $executor->close();
            $connection->close();
        }
    }

    public function testExclusiveOwnershipRejectsLegacyUseAndIsReleasedOnClose(): void
    {
        $connection = $this->connect(':memory:');
        $executor = $connection->createWorkerOperationExecutor(self::RESULT_BYTES, self::RESULT_ROWS);
        try {
            try {
                $connection->query('SELECT value FROM counter');
                self::fail('Legacy SQL must not enter an owned connection');
            } catch (SqliteTransactionError $error) {
                self::assertSame('A worker operation executor owns the connection', $error->getMessage());
            }
            $executor->close();
            self::assertSame(['value' => self::INITIAL_VALUE], $connection->query('SELECT value FROM counter')->fetchRow());
        } finally {
            $executor->close();
            $connection->close();
        }
    }

    public function testReadOnlyRejectsWritesAndConfigurationAndFinalizesLaterBatches(): void
    {
        $connection = $this->connect(':memory:');
        $executor = $connection->createWorkerOperationExecutor(self::RESULT_BYTES, self::RESULT_ROWS);
        try {
            foreach (['UPDATE counter SET value = 1', 'COMMIT', 'PRAGMA count_changes = ON'] as $sql) {
                try {
                    $executor->run('sql', ['sql' => $sql, 'bindings' => [], 'outcome' => Outcome::ReadOnly->value], Mode::ReadOnly);
                    self::fail('Read-only task must not execute control or write SQL');
                } catch (SqliteQueryError $error) {
                    self::assertSame($sql, $error->getQuery());
                    self::assertNull($error->getResultCode());
                }
            }
            $result = $executor->run('sql', ['sql' => 'WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<3) SELECT x FROM n', 'bindings' => [], 'outcome' => Outcome::ReadOnly->value], Mode::ReadOnly);
            self::assertSame([['x' => 1], ['x' => 2], ['x' => 3]], $result->data['rows']);
            self::assertNull($result->data['result_id']);
            self::assertTrue($result->data['exhausted']);
        } finally {
            $executor->close();
            $connection->close();
        }
    }

    public function testResultBoundsFailBeforeCommit(): void
    {
        $connection = $this->connect(':memory:');
        try {
            $executor = $connection->createWorkerOperationExecutor(self::RESULT_BYTES, self::RESULT_ROWS);
            try {
                $executor->run('counter', ['decrement' => 3, 'reject' => false, 'padding' => \str_repeat('x', self::RESULT_BYTES)], Mode::Transaction);
                self::fail('Expected result byte limit failure');
            } catch (SqliteQueryError $error) {
                self::assertNull($error->getResultCode());
            } finally {
                $executor->close();
            }
            self::assertSame(['value' => self::INITIAL_VALUE], $connection->query('SELECT value FROM counter')->fetchRow());
            $executor = $connection->createWorkerOperationExecutor(self::RESULT_BYTES, 1);
            try {
                $executor->run('sql', ['sql' => 'SELECT 1 UNION ALL SELECT 2', 'bindings' => [], 'outcome' => Outcome::Commit->value], Mode::Transaction);
                self::fail('Expected result row limit failure');
            } catch (SqliteQueryError $error) {
                self::assertStringContainsString('row limit', $error->getMessage());
            } finally {
                $executor->close();
            }
            self::assertSame(['value' => self::INITIAL_VALUE], $connection->query('SELECT value FROM counter')->fetchRow());
        } finally {
            $connection->close();
        }
    }

    public function testExecutorAcquisitionRejectsLegacyTransactionsAndUnreadResources(): void
    {
        $connection = $this->connect(':memory:');
        try {
            $transaction = $connection->beginTransaction();
            try {
                $connection->createWorkerOperationExecutor(self::RESULT_BYTES, self::RESULT_ROWS);
                self::fail('An executor must not enter a legacy transaction');
            } catch (SqliteTransactionError $error) {
                self::assertStringContainsString('legacy transaction', $error->getMessage());
            } finally {
                $transaction->rollback();
            }
            $rows = $connection->query('SELECT 1 UNION ALL SELECT 2');
            try {
                $connection->createWorkerOperationExecutor(self::RESULT_BYTES, self::RESULT_ROWS);
                self::fail('An executor must not wait on a resource owned by its caller');
            } catch (SqliteTransactionError $error) {
                self::assertStringContainsString('unread resources', $error->getMessage());
            } finally {
                $rows->close();
            }
            $connection->createWorkerOperationExecutor(self::RESULT_BYTES, self::RESULT_ROWS)->close();
        } finally {
            $connection->close();
        }
    }

    public function testUnregisteredOperationIsRejectedLocallyWithoutDamagingTheExecutor(): void
    {
        $connection = $this->connect(':memory:');
        $executor = $connection->createWorkerOperationExecutor(self::RESULT_BYTES, self::RESULT_ROWS);
        try {
            try {
                $executor->run('unknown', [], Mode::Transaction);
                self::fail('Unknown operation must be rejected');
            } catch (\InvalidArgumentException $error) {
                self::assertSame('Worker operation is not registered', $error->getMessage());
            }
            self::assertSame(Outcome::Commit, $executor->run('counter', ['decrement' => 0, 'reject' => false], Mode::Transaction)->outcome);
        } finally {
            $executor->close();
            $connection->close();
        }
    }

    public function testReadOnlyDoesNotAcquireAWriteTransaction(): void
    {
        $path = \tempnam(\sys_get_temp_dir(), 'worker-operation-');
        self::assertIsString($path);
        $connection = $this->connect($path);
        $observer = new \SQLite3($path);
        $observer->enableExceptions(true);
        $observer->exec('BEGIN IMMEDIATE');
        $executor = $connection->createWorkerOperationExecutor(self::RESULT_BYTES, self::RESULT_ROWS);
        try {
            $result = async(static fn () => $executor->run('sql', ['sql' => 'SELECT value FROM counter', 'bindings' => [], 'outcome' => Outcome::ReadOnly->value], Mode::ReadOnly))->await(new TimeoutCancellation(5));
            self::assertSame([['value' => self::INITIAL_VALUE]], $result->data['rows']);
        } finally {
            $observer->exec('ROLLBACK');
            $observer->close();
            $executor->close();
            $connection->close();
            @\unlink($path);
        }
    }

    public function testConnectionCloseInterruptsAnActiveExecutorWithoutWaitingForItsLease(): void
    {
        $endpoint = 'unix://' . \sys_get_temp_dir() . '/operation-barrier-' . \bin2hex(\random_bytes(8));
        $server = listen($endpoint);
        $connection = $this->connect(':memory:');
        $executor = $connection->createWorkerOperationExecutor(self::RESULT_BYTES, self::RESULT_ROWS);
        $running = async(static fn () => $executor->run('pause', ['endpoint' => $endpoint], Mode::Transaction));
        $peer = $server->accept(new TimeoutCancellation(5));
        self::assertNotNull($peer);
        self::assertSame("ready\n", $peer->read(new TimeoutCancellation(5)));
        $budget = new DeferredCancellation();
        $closing = async(static fn () => $connection->close($budget->getCancellation()));
        try {
            $closing->await(new TimeoutCancellation(5));
            try {
                $running->await(new TimeoutCancellation(5));
                self::fail('Closing must fail the active operation');
            } catch (SqliteConnectionException) {
                self::assertTrue($connection->isClosed());
                self::assertTrue($executor->isClosed());
            }
        } finally {
            $peer->close();
            $server->close();
            $budget->cancel();
            $connection->close();
            @\unlink(\substr($endpoint, \strlen('unix://')));
        }
    }
}
