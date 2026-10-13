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

use Fabpot\Amp\Sqlite\Worker\DTO\OperationResultDTO;
use Fabpot\Amp\Sqlite\Worker\Operation;
use Fabpot\Amp\Sqlite\Worker\Outcome;
use Fabpot\Amp\Sqlite\Worker\SqlExecutor;

/** Test-only child barrier; production operations do not perform control-channel I/O. */
final class PausedWorkerOperation implements Operation
{
    public function __construct(private readonly SqlExecutor $sql)
    {
    }

    public static function create(SqlExecutor $sql): self
    {
        return new self($sql);
    }

    public function run(array $parameters): OperationResultDTO
    {
        $stream = \stream_socket_client($parameters['endpoint']);
        if ($stream === false) {
            throw new \RuntimeException('Could not connect the worker test barrier');
        }
        try {
            \fwrite($stream, "ready\n");
            if (\fgets($stream) !== "release\n") {
                throw new \RuntimeException('The worker test barrier was closed');
            }
        } finally {
            \fclose($stream);
        }
        $value = $this->sql->execute('SELECT 1 AS value', []);

        return new OperationResultDTO(Outcome::Commit, $value);
    }
}
