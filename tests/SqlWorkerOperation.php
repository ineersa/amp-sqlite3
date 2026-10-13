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

/** Test-only single-statement fixture; not an application SQL programming language. */
final class SqlWorkerOperation implements Operation
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
        $value = $this->sql->execute($parameters['sql'], $parameters['bindings']);

        return new OperationResultDTO(Outcome::from($parameters['outcome']), $value);
    }
}
