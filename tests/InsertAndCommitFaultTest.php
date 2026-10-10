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
use Amp\DeferredFuture;
use Amp\Parallel\Context\Context;
use Amp\Parallel\Context\ContextFactory;
use Amp\Parallel\Context\ProcessContextFactory;
use Amp\TimeoutCancellation;
use Fabpot\Amp\Sqlite\Internal\Connection;
use Fabpot\Amp\Sqlite\Internal\ConnectionLeases;
use Fabpot\Amp\Sqlite\Internal\Transaction;
use Fabpot\Amp\Sqlite\SqliteConfig;
use Fabpot\Amp\Sqlite\SqliteConnectionException;
use Fabpot\Amp\Sqlite\SqliteConnector;
use Fabpot\Amp\Sqlite\SqliteTransactionError;
use Fabpot\Amp\Sqlite\SqliteTransactionMode;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use function Amp\async;
use function Amp\Socket\listen;

enum InsertFaultScenario: string
{
    case BeforeCommit = 'die';
    case LostReply = 'close';
    case MalformedReply = 'malformed';
}

final class InsertAndCommitFaultTest extends TestCase
{
    private const SAFETY_TIMEOUT_SECONDS = 5;

    #[DataProvider('faults')]
    public function testRealWorkerFaultPreservesKnownDatabaseOutcome(InsertFaultScenario $scenario): void
    {
        $directory = \sys_get_temp_dir().'/insert-fault-'.\bin2hex(\random_bytes(8));
        self::assertTrue(\mkdir($directory));
        try {
            $runtime = \file_get_contents(__DIR__.'/../src/Internal/WorkerProcess.php');
            $worker = \file_get_contents(__DIR__.'/../src/Internal/worker.php');
            self::assertIsString($runtime);
            self::assertIsString($worker);
            $insertHook = "        \$value = \$this->execute(\$request);\n        // A non-insert";
            self::assertSame(1, \substr_count($runtime, $insertHook));
            $runtime = \str_replace($insertHook, "        \$value = \$this->execute(\$request);\n        \$GLOBALS['insertFaultBarrier']->at('inserted');\n        // A non-insert", $runtime);
            $commitHook = "            \$this->database->exec('COMMIT');\n        } catch (\\SQLite3Exception";
            self::assertSame(1, \substr_count($runtime, $commitHook));
            $runtime = \str_replace($commitHook, "            \$this->database->exec('COMMIT');\n            \$GLOBALS['insertFaultBarrier']->at('committed');\n        } catch (\\SQLite3Exception", $runtime);
            $replyHook = "            \$channel->send(['id' => \$id, 'value' => \$worker->handle(\$request)]);";
            self::assertSame(1, \substr_count($worker, $replyHook));
            $worker = \str_replace($replyHook, "            \$value = \$worker->handle(\$request);\n            if (\$request['operation'] === 'executeInsertAndCommit') {\n                \$value = \$GLOBALS['insertFaultBarrier']->reply(\$value);\n            }\n            \$channel->send(['id' => \$id, 'value' => \$value]);", $worker);
            \file_put_contents($directory.'/WorkerProcess.php', $runtime);
            \file_put_contents($directory.'/worker.php', $worker);
            $database = $directory.'/queue.sqlite';
            $server = listen('127.0.0.1:0');
            try {
                $uri = 'tcp://'.$server->getAddress()->toString();
                $factory = new InsertFaultContextFactory(new ProcessContextFactory(), $directory, $uri);
                $connection = (new SqliteConnector($factory))->connect(new SqliteConfig($database));
                try {
                    self::assertInstanceOf(Connection::class, $connection);
                    $connection->setTransactionIsolation(SqliteTransactionMode::Immediate);
                    $connection->executeScript('CREATE TABLE entries (id INTEGER PRIMARY KEY, body BLOB NOT NULL)');
                    $socket = $server->accept(new TimeoutCancellation(self::SAFETY_TIMEOUT_SECONDS));
                    self::assertNotNull($socket);
                    $control = new StreamChannel($socket, $socket);
                    try {
                        $transaction = $connection->beginTransaction();
                        self::assertInstanceOf(Transaction::class, $transaction);
                        $successCalls = 0;
                        $closed = new DeferredFuture();
                        $transaction->onCommit(static function () use (&$successCalls): void {
                            ++$successCalls;
                        });
                        $transaction->onClose(static function () use ($closed): void {
                            $closed->complete();
                        });
                        $future = async(static fn () => $transaction->executeInsertAndCommit("INSERT INTO entries (body) VALUES (X'0071')", []));
                        self::assertSame('inserted', $control->receive(new TimeoutCancellation(self::SAFETY_TIMEOUT_SECONDS)));
                        self::assertFalse($future->isComplete());
                        if ($scenario === InsertFaultScenario::BeforeCommit) {
                            $control->send($scenario->value);
                        } else {
                            $control->send('continue');
                            self::assertSame('committed', $control->receive(new TimeoutCancellation(self::SAFETY_TIMEOUT_SECONDS)));
                            self::assertFalse($future->isComplete());
                            // The worker is parked after native COMMIT, before any success reply.
                            $reader = new \SQLite3($database, SQLITE3_OPEN_READONLY);
                            try {
                                self::assertSame(1, $reader->querySingle('SELECT count(*) FROM entries'));
                            } finally {
                                $reader->close();
                            }
                            $control->send($scenario->value);
                        }
                        try {
                            $future->await(new TimeoutCancellation(self::SAFETY_TIMEOUT_SECONDS));
                            self::fail('Fault must not produce a successful insert confirmation');
                        } catch (SqliteConnectionException) {
                            self::assertTrue($connection->isClosed());
                            self::assertFalse($transaction->isActive());
                        }
                        $closed->getFuture()->await(new TimeoutCancellation(self::SAFETY_TIMEOUT_SECONDS));
                        self::assertSame(0, $successCalls);
                        $leases = (new \ReflectionProperty(Connection::class, 'leases'))->getValue($connection);
                        self::assertInstanceOf(ConnectionLeases::class, $leases);
                        self::assertFalse($leases->isBusy());
                        self::assertNull((new \ReflectionProperty(Connection::class, 'activeTransaction'))->getValue($connection));
                        try {
                            $transaction->rollback();
                            self::fail('Closed transaction must reject rollback before dispatch');
                        } catch (SqliteTransactionError) {
                        }
                        try {
                            $connection->query('SELECT 1');
                            self::fail('Broken connection must not be reused');
                        } catch (SqliteConnectionException) {
                        }
                        self::assertSame(1, $factory->starts);
                        $reopened = (new SqliteConnector())->connect(new SqliteConfig($database));
                        try {
                            // A new IMMEDIATE transaction proves external SQLite writer ownership is free.
                            $reopened->setTransactionIsolation(SqliteTransactionMode::Immediate);
                            $readTransaction = $reopened->beginTransaction();
                            $result = $readTransaction->query('SELECT count(*) AS count FROM entries');
                            try {
                                self::assertSame($scenario === InsertFaultScenario::BeforeCommit ? 0 : 1, $result->fetchRow()['count']);
                            } finally {
                                $result->close();
                            }
                            $readTransaction->rollback();
                        } finally {
                            $reopened->close();
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
            foreach (\glob($directory.'/*') as $file) {
                \unlink($file);
            }
            \rmdir($directory);
        }
    }

    public static function faults(): iterable
    {
        foreach (InsertFaultScenario::cases() as $scenario) {
            yield $scenario->name => [$scenario];
        }
    }
}

final class InsertFaultContextFactory implements ContextFactory
{
    public int $starts = 0;

    public function __construct(private readonly ContextFactory $factory, private readonly string $directory, private readonly string $uri)
    {
    }

    public function start(string|array $script, ?Cancellation $cancellation = null): Context
    {
        ++$this->starts;

        return $this->factory->start([__DIR__.'/Fixture/insert-fault-worker.php', $this->directory.'/WorkerProcess.php', $this->directory.'/worker.php', $this->uri], $cancellation);
    }
}
