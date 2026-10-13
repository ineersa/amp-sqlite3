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

final class CounterWorkerOperation implements Operation
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
        $row = $this->sql->execute('SELECT value FROM counter', [])['rows'][0];
        $decrement = $parameters['decrement'];
        if (!\is_int($decrement)) {
            throw new \RuntimeException('Counter decrement must be an integer');
        }
        $value = $row['value'] - $decrement;
        $this->sql->execute('UPDATE counter SET value = ?', [$value]);
        if ($parameters['reject']) {
            throw new \RuntimeException('Counter validation failed after update');
        }

        // Optional test-only padding exercises final reply bounds after the UPDATE.
        $data = ['value' => $value];
        if (\array_key_exists('padding', $parameters)) {
            $data['padding'] = $parameters['padding'];
        }

        return new OperationResultDTO($value < 0 ? Outcome::Rollback : Outcome::Commit, $data);
    }
}
