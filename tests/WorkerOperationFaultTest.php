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

use Amp\ByteStream\StreamChannel;
use Amp\Cancellation;
use Amp\Parallel\Context\Context;
use Amp\Parallel\Context\ContextFactory;
use Amp\Parallel\Context\ProcessContextFactory;
use Amp\TimeoutCancellation;
use Fabpot\Amp\Sqlite\Internal\Connection;
use Fabpot\Amp\Sqlite\Internal\ConnectionLeases;
use Fabpot\Amp\Sqlite\SqliteConfig;
use Fabpot\Amp\Sqlite\SqliteConnectionException;
use Fabpot\Amp\Sqlite\SqliteConnector;
use Fabpot\Amp\Sqlite\SqliteQueryError;
use Fabpot\Amp\Sqlite\Worker\Mode;
use Fabpot\Amp\Sqlite\Worker\Outcome;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use function Amp\async;
use function Amp\Socket\listen;

enum WorkerOperationFaultScenario: string
{
    case BeforeCommit = 'exit_before_commit';
    case LostReply = 'lose_reply';
    case MalformedReply = 'malformed_reply';
}

final class WorkerOperationFaultTest extends TestCase
{
    private const SAFETY_TIMEOUT_SECONDS = 5;
    private const INITIAL_VALUE = 10;
    private const RESULT_BYTES = 16_384;
    private const RESULT_ROWS = 1;
    private const DATABASE_PAGE_BYTES = 512;
    private const OVERSIZED_BLOB_BYTES = 8192;
    private const SQLITE_FULL = 13;
    private const SQLITE_ERROR = 1;

    public function testSqliteFullPreservesPrimaryFailureAndRollbackDiagnostic(): void
    {
        $directory = $this->directory();
        $path = $directory . '/full.sqlite';
        $connection = (new SqliteConnector())->connect((new SqliteConfig($path))
            ->withPragma('page_size', self::DATABASE_PAGE_BYTES)
            ->withWorkerOperations(['sql' => SqlWorkerOperation::class]));
        try {
            $connection->query('CREATE TABLE entries (body BLOB NOT NULL)')->close();
            $pages = $connection->query('PRAGMA page_count');
            try {
                $pageCount = $pages->fetchRow()['page_count'];
                self::assertIsInt($pageCount);
            } finally {
                $pages->close();
            }
            $connection->query('PRAGMA max_page_count = ' . $pageCount)->close();
            $executor = $connection->createWorkerOperationExecutor(self::RESULT_BYTES, self::RESULT_ROWS, new \Fabpot\Amp\Sqlite\Worker\DTO\PipelineLimitsDTO(4, 256 * 1024, 8, 512 * 1024, 8192));
            $sql = 'INSERT INTO entries (body) VALUES (zeroblob(?))';
            try {
                $executor->run('sql', ['sql' => $sql, 'bindings' => [self::OVERSIZED_BLOB_BYTES], 'outcome' => Outcome::Commit->value], Mode::Transaction);
                self::fail('Expected SQLITE_FULL to abort the transaction automatically');
            } catch (SqliteQueryError $error) {
                self::assertSame($sql, $error->getQuery());
                self::assertSame(self::SQLITE_FULL, $error->getResultCode());
                self::assertSame(self::SQLITE_FULL, $error->getExtendedResultCode());
                $cleanup = $error->getPrevious();
                self::assertInstanceOf(SqliteQueryError::class, $cleanup);
                self::assertSame('ROLLBACK', $cleanup->getQuery());
                self::assertSame(self::SQLITE_ERROR, $cleanup->getResultCode());
                self::assertSame(self::SQLITE_ERROR, $cleanup->getExtendedResultCode());
                self::assertStringContainsString('no transaction is active', $cleanup->getMessage());
                self::assertTrue($connection->isClosed());
                self::assertTrue($executor->isClosed());
            }
            $this->assertReleased($connection);
            $this->assertCannotReuse($connection);
            $observer = new \SQLite3($path);
            $observer->enableExceptions(true);
            try {
                $observer->exec('BEGIN IMMEDIATE');
                self::assertSame(0, $observer->querySingle('SELECT count(*) FROM entries'));
                $observer->exec('ROLLBACK');
            } finally {
                $observer->close();
            }
        } finally {
            $connection->close();
            $this->removeDirectory($directory);
        }
    }

    #[DataProvider('scenarios')]
    public function testRealWorkerMutationAndCommitFaults(WorkerOperationFaultScenario $scenario): void
    {
        $directory = $this->directory();
        try {
            $runtime = \file_get_contents(__DIR__ . '/../src/Internal/WorkerProcess.php');
            $worker = \file_get_contents(__DIR__ . '/../src/Internal/worker.php');
            self::assertIsString($runtime);
            self::assertIsString($worker);
            $mutationAnchor = "            \$result = \$operation->run(\$parameters);\n            if ((\$mode ===";
            self::assertSame(1, \substr_count($runtime, $mutationAnchor));
            $runtime = \str_replace($mutationAnchor, "            \$result = \$operation->run(\$parameters);\n            \$GLOBALS['operationFaultBarrier']->at('mutated');\n            if ((\$mode ===", $runtime);
            $commitAnchor = "                \$this->workerControl(\$result->outcome === \\Fabpot\\Amp\\Sqlite\\Worker\\Outcome::Commit ? 'COMMIT' : 'ROLLBACK');";
            self::assertSame(1, \substr_count($runtime, $commitAnchor));
            $runtime = \str_replace($commitAnchor, $commitAnchor . "\n                if (\$result->outcome === \\Fabpot\\Amp\\Sqlite\\Worker\\Outcome::Commit) {\n                    \$GLOBALS['operationFaultBarrier']->at('committed');\n                }", $runtime);
            $replyAnchor = "            \$channel->send(['id' => \$id, 'value' => \$worker->handle(\$request)]);";
            self::assertSame(1, \substr_count($worker, $replyAnchor));
            $worker = \str_replace($replyAnchor, "            \$value = \$worker->handle(\$request);\n            if (\$request['operation'] === 'executeWorkerOperation') {\n                \$value = \$GLOBALS['operationFaultBarrier']->reply(\$value);\n            }\n            \$channel->send(['id' => \$id, 'value' => \$value]);", $worker);
            self::assertNotFalse(\file_put_contents($directory . '/WorkerProcess.php', $runtime));
            self::assertNotFalse(\file_put_contents($directory . '/worker.php', $worker));
            $path = $directory . '/fault.sqlite';
            $server = listen('127.0.0.1:0');
            try {
                $factory = new WorkerOperationFaultContextFactory(new ProcessContextFactory(), $directory, 'tcp://' . $server->getAddress()->toString());
                $connection = (new SqliteConnector($factory))->connect((new SqliteConfig($path))->withWorkerOperations(['sql' => SqlWorkerOperation::class]));
                try {
                    $connection->query('CREATE TABLE counter (value INTEGER NOT NULL)')->close();
                    $connection->execute('INSERT INTO counter VALUES (?)', [self::INITIAL_VALUE])->close();
                    $socket = $server->accept(new TimeoutCancellation(self::SAFETY_TIMEOUT_SECONDS));
                    self::assertNotNull($socket);
                    $control = new StreamChannel($socket, $socket);
                    try {
                        $executor = $connection->createWorkerOperationExecutor(self::RESULT_BYTES, self::RESULT_ROWS, new \Fabpot\Amp\Sqlite\Worker\DTO\PipelineLimitsDTO(4, 256 * 1024, 8, 512 * 1024, 8192));
                        $confirmations = 0;
                        $future = async(static function () use ($executor, &$confirmations): void {
                            $executor->run('sql', ['sql' => 'UPDATE counter SET value = value + 1', 'bindings' => [], 'outcome' => Outcome::Commit->value], Mode::Transaction);
                            ++$confirmations;
                        });
                        self::assertSame('mutated', $control->receive(new TimeoutCancellation(self::SAFETY_TIMEOUT_SECONDS)));
                        self::assertFalse($future->isComplete());
                        self::assertSame(self::INITIAL_VALUE, $this->readValue($path));
                        if ($scenario === WorkerOperationFaultScenario::BeforeCommit) {
                            $control->send($scenario->value);
                        } else {
                            $control->send('continue');
                            self::assertSame('committed', $control->receive(new TimeoutCancellation(self::SAFETY_TIMEOUT_SECONDS)));
                            self::assertFalse($future->isComplete());
                            self::assertSame(self::INITIAL_VALUE + 1, $this->readValue($path));
                            $control->send($scenario->value);
                        }
                        try {
                            $future->await(new TimeoutCancellation(self::SAFETY_TIMEOUT_SECONDS));
                            self::fail('A fault must not produce a successful confirmation');
                        } catch (SqliteConnectionException) {
                            self::assertTrue($connection->isClosed());
                            self::assertTrue($executor->isClosed());
                        }
                        self::assertSame(0, $confirmations);
                        self::assertSame(1, $factory->starts);
                        $this->assertReleased($connection);
                        $this->assertCannotReuse($connection);
                        try {
                            $executor->run('sql', ['sql' => 'UPDATE counter SET value = value + 1', 'bindings' => [], 'outcome' => Outcome::Commit->value], Mode::Transaction);
                            self::fail('An old executor must not replay on a replacement connection');
                        } catch (SqliteConnectionException) {
                        }
                        $observer = new \SQLite3($path);
                        $observer->enableExceptions(true);
                        // Force termination and process reaping are asynchronous. Successful
                        // transaction acquisition proves release; this budget only aborts a leak.
                        $observer->busyTimeout(self::SAFETY_TIMEOUT_SECONDS * 1000);
                        try {
                            $observer->exec('BEGIN IMMEDIATE');
                            self::assertSame($scenario === WorkerOperationFaultScenario::BeforeCommit ? self::INITIAL_VALUE : self::INITIAL_VALUE + 1, $observer->querySingle('SELECT value FROM counter'));
                            $observer->exec('ROLLBACK');
                        } finally {
                            $observer->close();
                        }
                    } finally {
                        $control->close();
                    }
                } finally {
                    $connection->close();
                }
            } finally {
                $server->close();
            }
        } finally {
            $this->removeDirectory($directory);
        }
    }

    public static function scenarios(): iterable
    {
        foreach (WorkerOperationFaultScenario::cases() as $scenario) {
            yield $scenario->name => [$scenario];
        }
    }

    private function assertReleased(Connection $connection): void
    {
        $leases = (new \ReflectionProperty(Connection::class, 'leases'))->getValue($connection);
        self::assertInstanceOf(ConnectionLeases::class, $leases);
        self::assertFalse($leases->isBusy());
    }

    private function assertCannotReuse(Connection $connection): void
    {
        try {
            $connection->query('SELECT 1');
            self::fail('The unusable connection must reject further requests');
        } catch (SqliteConnectionException) {
        }
    }

    private function readValue(string $path): int
    {
        $observer = new \SQLite3($path, SQLITE3_OPEN_READONLY);
        try {
            $value = $observer->querySingle('SELECT value FROM counter');
            self::assertIsInt($value);

            return $value;
        } finally {
            $observer->close();
        }
    }

    private function directory(): string
    {
        $directory = \sys_get_temp_dir() . '/operation-fault-' . \bin2hex(\random_bytes(8));
        self::assertTrue(\mkdir($directory));

        return $directory;
    }

    private function removeDirectory(string $directory): void
    {
        foreach (\glob($directory . '/*') as $path) {
            \unlink($path);
        }
        \rmdir($directory);
    }
}

final class WorkerOperationFaultContextFactory implements ContextFactory
{
    public int $starts = 0;

    public function __construct(private readonly ContextFactory $factory, private readonly string $directory, private readonly string $uri)
    {
    }

    public function start(string|array $script, ?Cancellation $cancellation = null): Context
    {
        ++$this->starts;

        return $this->factory->start([__DIR__ . '/Fixture/operation-fault-worker.php', $this->directory . '/WorkerProcess.php', $this->directory . '/worker.php', $this->uri], $cancellation);
    }
}
