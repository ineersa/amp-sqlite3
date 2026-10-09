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

use Fabpot\Amp\Sqlite\Internal\WorkerProcess;
use Fabpot\Amp\Sqlite\SqliteBlob;
use Fabpot\Amp\Sqlite\SqliteConfig;
use Fabpot\Amp\Sqlite\SqliteConnection;
use Fabpot\Amp\Sqlite\SqliteConnector;
use Fabpot\Amp\Sqlite\SqliteJournalMode;
use Fabpot\Amp\Sqlite\SqliteOpenMode;
use Fabpot\Amp\Sqlite\SqliteQueryError;
use Fabpot\Amp\Sqlite\SqliteSynchronousMode;
use PHPUnit\Framework\TestCase;

final class SqliteStatementCacheTest extends TestCase
{
    public function testResidentReuseAvoidsAdditionalUserPreparations(): void
    {
        $worker = $this->createWorker(statementCacheSize: 8);
        $sql = 'SELECT ? AS value';

        $first = $worker->handle(['operation' => 'execute', 'sql' => $sql, 'params' => [1], 'bind_parameters' => true]);
        self::assertSame(1, $worker->getUserStatementPreparations());
        self::assertSame([['value' => 1]], $first['rows']);
        self::assertTrue($first['exhausted']);

        $second = $worker->handle(['operation' => 'execute', 'sql' => $sql, 'params' => [2], 'bind_parameters' => true]);
        self::assertSame(1, $worker->getUserStatementPreparations());
        self::assertSame([['value' => 2]], $second['rows']);

        $worker->handle(['operation' => 'close']);
    }

    public function testDisabledCachePreparesOnEveryDirectExecution(): void
    {
        $worker = $this->createWorker(statementCacheSize: 0);
        $sql = 'SELECT 1 AS value';

        $worker->handle(['operation' => 'execute', 'sql' => $sql, 'params' => [], 'bind_parameters' => true]);
        $worker->handle(['operation' => 'execute', 'sql' => $sql, 'params' => [], 'bind_parameters' => true]);

        self::assertSame(2, $worker->getUserStatementPreparations());
        $worker->handle(['operation' => 'close']);
    }

    public function testCapacityEvictsLeastRecentlyUsedIdleEntry(): void
    {
        $worker = $this->createWorker(statementCacheSize: 1);

        $worker->handle(['operation' => 'execute', 'sql' => 'SELECT 1 AS value', 'params' => [], 'bind_parameters' => true]);
        self::assertSame(1, $worker->getUserStatementPreparations());

        $worker->handle(['operation' => 'execute', 'sql' => 'SELECT 2 AS value', 'params' => [], 'bind_parameters' => true]);
        self::assertSame(2, $worker->getUserStatementPreparations());

        $worker->handle(['operation' => 'execute', 'sql' => 'SELECT 1 AS value', 'params' => [], 'bind_parameters' => true]);
        self::assertSame(3, $worker->getUserStatementPreparations());

        $worker->handle(['operation' => 'execute', 'sql' => 'SELECT 2 AS value', 'params' => [], 'bind_parameters' => true]);
        self::assertSame(4, $worker->getUserStatementPreparations());

        $worker->handle(['operation' => 'close']);
    }

    public function testOversizeSqlIsNotCached(): void
    {
        $worker = $this->createWorker(statementCacheSize: 8);
        $padding = \str_repeat('x', 4_100);
        $sql = "SELECT '{$padding}' AS value";

        $worker->handle(['operation' => 'execute', 'sql' => $sql, 'params' => [], 'bind_parameters' => true]);
        $worker->handle(['operation' => 'execute', 'sql' => $sql, 'params' => [], 'bind_parameters' => true]);

        self::assertSame(2, $worker->getUserStatementPreparations());
        $worker->handle(['operation' => 'close']);
    }

    public function testReuseSurvivesCommittedAndRolledBackTransactions(): void
    {
        $connection = $this->connect(statementCacheSize: 8);
        try {
            $connection->query('CREATE TABLE entries (id INTEGER PRIMARY KEY, value TEXT NOT NULL)');
            $sql = "INSERT INTO entries (value) VALUES (?)";

            $transaction = $connection->beginTransaction();
            self::assertSame(1, $transaction->execute($sql, ['a'])->getRowCount());
            $transaction->commit();

            $transaction = $connection->beginTransaction();
            self::assertSame(1, $transaction->execute($sql, ['b'])->getRowCount());
            $transaction->rollback();

            self::assertSame(1, $connection->execute($sql, ['c'])->getRowCount());
            self::assertSame(
                [['value' => 'a'], ['value' => 'c']],
                \iterator_to_array($connection->query('SELECT value FROM entries ORDER BY id')),
            );
        } finally {
            $connection->close();
        }
    }

    public function testOmittedParametersBecomeNullAndLargeBlobsDoNotLeak(): void
    {
        $connection = $this->connect(statementCacheSize: 8);
        try {
            $connection->query('CREATE TABLE entries (id INTEGER PRIMARY KEY, value BLOB)');
            $sql = 'INSERT INTO entries (value) VALUES (?)';
            $blob = new SqliteBlob(\str_repeat("\0blob", 8_192));

            self::assertSame(1, $connection->execute($sql, [$blob])->getRowCount());
            self::assertSame(1, $connection->execute($sql, [])->getRowCount());

            $rows = \iterator_to_array($connection->query('SELECT value FROM entries ORDER BY id'));
            self::assertInstanceOf(SqliteBlob::class, $rows[0]['value']);
            self::assertSame($blob->getBytes(), $rows[0]['value']->getBytes());
            self::assertNull($rows[1]['value']);
        } finally {
            $connection->close();
        }
    }

    public function testCachedExecuteDoesNotBypassQueryPlaceholderRejection(): void
    {
        $connection = $this->connect(statementCacheSize: 8);
        try {
            $sql = 'SELECT ? AS value';
            self::assertSame([['value' => 7]], \iterator_to_array($connection->execute($sql, [7])));

            $this->expectException(SqliteQueryError::class);
            $this->expectExceptionMessage('Parameters are not allowed in direct queries');
            $connection->query($sql);
        } finally {
            $connection->close();
        }
    }

    public function testActiveResultOwnsHandleUntilClosed(): void
    {
        $worker = $this->createWorker(statementCacheSize: 1, batchSize: 1);
        $sql = 'SELECT 1 AS value UNION ALL SELECT 2';

        $open = $worker->handle(['operation' => 'execute', 'sql' => $sql, 'params' => [], 'bind_parameters' => true]);
        self::assertFalse($open['exhausted']);
        self::assertNotNull($open['result_id']);
        self::assertSame(1, $worker->getUserStatementPreparations());

        // Capacity pressure must not close the open result's borrowed handle.
        $worker->handle(['operation' => 'execute', 'sql' => 'SELECT 3 AS value', 'params' => [], 'bind_parameters' => true]);
        self::assertSame(2, $worker->getUserStatementPreparations());

        $next = $worker->handle(['operation' => 'fetch', 'result_id' => $open['result_id']]);
        self::assertSame([['value' => 2]], $next['rows']);
        self::assertTrue($next['exhausted']);

        // After release, the original SQL can be reused from the idle cache again.
        $worker->handle(['operation' => 'execute', 'sql' => $sql, 'params' => [], 'bind_parameters' => true]);
        self::assertSame(2, $worker->getUserStatementPreparations());

        $worker->handle(['operation' => 'close']);
    }

    public function testBindingFailureDiscardsBorrowedHandleWithoutReplay(): void
    {
        $worker = $this->createWorker(statementCacheSize: 8);
        $sql = 'SELECT ? AS value';

        $worker->handle(['operation' => 'execute', 'sql' => $sql, 'params' => [1], 'bind_parameters' => true]);
        self::assertSame(1, $worker->getUserStatementPreparations());

        try {
            $worker->handle(['operation' => 'execute', 'sql' => $sql, 'params' => [new \stdClass()], 'bind_parameters' => true]);
            self::fail('Expected invalid parameter rejection');
        } catch (\Throwable $exception) {
            self::assertStringContainsString('params', $exception->getMessage());
        }

        $result = $worker->handle(['operation' => 'execute', 'sql' => $sql, 'params' => [2], 'bind_parameters' => true]);
        self::assertSame([['value' => 2]], $result['rows']);
        self::assertSame(2, $worker->getUserStatementPreparations());

        $worker->handle(['operation' => 'close']);
    }

    public function testSchemaChangeFlushesIdleCache(): void
    {
        $worker = $this->createWorker(statementCacheSize: 8);
        $sql = 'SELECT name FROM sqlite_master WHERE type = ? ORDER BY name';

        $worker->handle(['operation' => 'execute', 'sql' => $sql, 'params' => ['table'], 'bind_parameters' => true]);
        self::assertSame(1, $worker->getUserStatementPreparations());

        $worker->handle([
            'operation' => 'execute',
            'sql' => 'CREATE TABLE entries (id INTEGER PRIMARY KEY)',
            'params' => [],
            'bind_parameters' => true,
        ]);

        $worker->handle(['operation' => 'execute', 'sql' => $sql, 'params' => ['table'], 'bind_parameters' => true]);
        self::assertSame(3, $worker->getUserStatementPreparations());

        $worker->handle(['operation' => 'close']);
    }

    public function testPublicPreparedStatementsRemainSeparate(): void
    {
        $worker = $this->createWorker(statementCacheSize: 8);
        $sql = 'SELECT ? AS value';

        $prepared = $worker->handle(['operation' => 'prepare', 'sql' => $sql]);
        self::assertSame(1, $worker->getUserStatementPreparations());

        $worker->handle([
            'operation' => 'executeStatement',
            'statement_id' => $prepared['statement_id'],
            'params' => [1],
            'bind_parameters' => true,
        ]);
        self::assertSame(1, $worker->getUserStatementPreparations());

        $worker->handle(['operation' => 'execute', 'sql' => $sql, 'params' => [2], 'bind_parameters' => true]);
        self::assertSame(2, $worker->getUserStatementPreparations());

        $worker->handle(['operation' => 'closeStatement', 'statement_id' => $prepared['statement_id']]);
        $worker->handle(['operation' => 'close']);
    }

    private function connect(int $statementCacheSize, int $batchSize = 100): SqliteConnection
    {
        return (new SqliteConnector())->connect(
            (new SqliteConfig(':memory:'))
                ->withStatementCacheSize($statementCacheSize)
                ->withBatchSize($batchSize),
        );
    }

    private function createWorker(int $statementCacheSize, int $batchSize = 100): WorkerProcess
    {
        $worker = new WorkerProcess([
            'path' => ':memory:',
            'open_mode' => SqliteOpenMode::ReadWriteCreate->name,
            'journal_mode' => SqliteJournalMode::Automatic->value,
            'synchronous_mode' => SqliteSynchronousMode::Automatic->value,
            'foreign_keys' => true,
            'busy_timeout' => 5_000,
            'batch_size' => $batchSize,
            'statement_cache_size' => $statementCacheSize,
            'trusted_schema' => false,
            'extended_result_codes' => true,
            'pragmas' => [],
            'functions' => [],
            'aggregates' => [],
            'collations' => [],
        ]);
        $worker->enableUserStatementPreparationCounting();

        return $worker;
    }
}
