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

use Amp\DeferredFuture;
use Fabpot\Amp\Sqlite\Internal\Transaction;
use Fabpot\Amp\Sqlite\SqliteConfig;
use Fabpot\Amp\Sqlite\SqliteConnection;
use Fabpot\Amp\Sqlite\SqliteConnector;
use Fabpot\Amp\Sqlite\SqliteQueryError;
use Fabpot\Amp\Sqlite\SqliteTransactionError;
use Fabpot\Amp\Sqlite\SqliteTransactionMode;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class InsertAndCommitTest extends TestCase
{
    private SqliteConnection $connection;

    protected function setUp(): void
    {
        $this->connection = (new SqliteConnector())->connect((new SqliteConfig(':memory:'))->withBatchSize(1));
        $this->connection->setTransactionIsolation(SqliteTransactionMode::Immediate);
        $this->connection->executeScript('CREATE TABLE entries (value TEXT NOT NULL UNIQUE)');
    }

    protected function tearDown(): void
    {
        $this->connection->close();
    }

    public function testCommitsAndReleasesRootLeaseAndCallbacksOnce(): void
    {
        $transaction = $this->connection->beginTransaction();
        self::assertInstanceOf(Transaction::class, $transaction);
        $statement = $transaction->prepare('SELECT value FROM entries');
        $committed = new DeferredFuture();
        $closed = new DeferredFuture();
        $statementClosed = new DeferredFuture();
        $commitCalls = $closeCalls = $statementCloseCalls = $rollbackCalls = 0;
        $transaction->onCommit(static function () use ($committed, &$commitCalls): void {
            ++$commitCalls;
            $committed->complete();
        });
        $transaction->onRollback(static function () use (&$rollbackCalls): void {
            ++$rollbackCalls;
        });
        $transaction->onClose(static function () use ($closed, &$closeCalls): void {
            ++$closeCalls;
            $closed->complete();
        });
        $statement->onClose(static function () use ($statementClosed, &$statementCloseCalls): void {
            ++$statementCloseCalls;
            $statementClosed->complete();
        });
        self::assertSame(1, $transaction->executeInsertAndCommit('INSERT INTO entries VALUES (?)', ['committed']));
        self::assertFalse($transaction->isActive());
        self::assertTrue($statement->isClosed());
        $committed->getFuture()->await();
        $closed->getFuture()->await();
        $statementClosed->getFuture()->await();
        $transaction->close();
        $statement->close();
        self::assertSame([1, 1, 1, 0], [$commitCalls, $closeCalls, $statementCloseCalls, $rollbackCalls]);
        // Starting another root transaction proves the connection transaction lock was released.
        $next = $this->connection->beginTransaction();
        self::assertSame([['value' => 'committed']], \iterator_to_array($next->query('SELECT value FROM entries')));
        $next->rollback();
    }

    public function testKnownSqlFailureLeavesTransactionActiveForRollback(): void
    {
        $transaction = $this->connection->beginTransaction();
        self::assertInstanceOf(Transaction::class, $transaction);
        $transaction->execute('INSERT INTO entries VALUES (?)', ['pending'])->close();
        try {
            $transaction->executeInsertAndCommit('INSERT INTO entries VALUES (?)', ['pending']);
            self::fail('Duplicate insert must fail');
        } catch (SqliteQueryError) {
            self::assertTrue($transaction->isActive());
            $transaction->rollback();
        }
        self::assertSame([], \iterator_to_array($this->connection->query('SELECT value FROM entries')));
    }

    #[DataProvider('invalidInsertIds')]
    public function testInvalidIdDoesNotCommit(string $sql): void
    {
        $transaction = $this->connection->beginTransaction();
        self::assertInstanceOf(Transaction::class, $transaction);
        $transaction->execute('INSERT INTO entries VALUES (?)', ['pending'])->close();
        try {
            $transaction->executeInsertAndCommit($sql, []);
            self::fail('Missing or non-positive insert ID must fail');
        } catch (SqliteQueryError $error) {
            self::assertStringContainsString('positive native insert ID', $error->getMessage());
            self::assertTrue($transaction->isActive());
            $transaction->rollback();
        }
        self::assertSame([], \iterator_to_array($this->connection->query('SELECT value FROM entries')));
    }

    public static function invalidInsertIds(): iterable
    {
        yield 'zero' => ["INSERT INTO entries (rowid, value) VALUES (0, 'zero')"];
        yield 'negative' => ["INSERT INTO entries (rowid, value) VALUES (-1, 'negative')"];
        yield 'ignored insert' => ["INSERT OR IGNORE INTO entries VALUES ('pending')"];
        yield 'live non-insert cursor' => ['SELECT 1 UNION ALL SELECT 2'];
    }

    public function testCommitSqlFailureRemainsRollbackable(): void
    {
        $this->connection->executeScript('CREATE TABLE parents (id INTEGER PRIMARY KEY); CREATE TABLE children (parent_id INTEGER REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED)');
        $transaction = $this->connection->beginTransaction();
        self::assertInstanceOf(Transaction::class, $transaction);
        try {
            $transaction->executeInsertAndCommit('INSERT INTO children VALUES (?)', [1]);
            self::fail('Deferred foreign key must reject COMMIT');
        } catch (SqliteQueryError) {
            self::assertTrue($transaction->isActive());
            $transaction->rollback();
        }
        self::assertSame([], \iterator_to_array($this->connection->query('SELECT parent_id FROM children')));
    }

    public function testCurrentTaskResultLeasePreventsCombinedCommit(): void
    {
        $transaction = $this->connection->beginTransaction();
        self::assertInstanceOf(Transaction::class, $transaction);
        $result = $transaction->query('SELECT 1 UNION ALL SELECT 2');
        try {
            $transaction->executeInsertAndCommit('INSERT INTO entries VALUES (?)', ['blocked']);
            self::fail('Open result must prevent transaction finalization');
        } catch (SqliteTransactionError) {
            self::assertTrue($transaction->isActive());
        }
        $result->close();
        self::assertSame(1, $transaction->executeInsertAndCommit('INSERT INTO entries VALUES (?)', ['allowed']));
    }

    public function testRootWithActiveNestedTransactionRejectsCombinedCommit(): void
    {
        $root = $this->connection->beginTransaction();
        self::assertInstanceOf(Transaction::class, $root);
        $nested = $root->beginTransaction();
        try {
            $root->executeInsertAndCommit('INSERT INTO entries VALUES (?)', ['blocked']);
            self::fail('Active nested transaction must prevent root finalization');
        } catch (SqliteTransactionError) {
            self::assertTrue($root->isActive());
            self::assertTrue($nested->isActive());
        }
        $nested->rollback();
        $root->rollback();
    }

    public function testNestedTransactionIsRejectedWithoutChangingEitherTransaction(): void
    {
        $root = $this->connection->beginTransaction();
        $nested = $root->beginTransaction();
        self::assertInstanceOf(Transaction::class, $nested);
        try {
            $nested->executeInsertAndCommit('INSERT INTO entries VALUES (?)', ['nested']);
            self::fail('Nested combined commit must fail');
        } catch (SqliteTransactionError) {
            self::assertTrue($root->isActive());
            self::assertTrue($nested->isActive());
        }
        $nested->rollback();
        $root->rollback();
        self::assertSame([], \iterator_to_array($this->connection->query('SELECT value FROM entries')));
    }
}
