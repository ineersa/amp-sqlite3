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

namespace Fabpot\Amp\Sqlite\Worker\DTO;

use Fabpot\Amp\Sqlite\Worker\Outcome;

final readonly class OperationResultDTO
{
    /** @param array<array-key, mixed> $data */
    public function __construct(public Outcome $outcome, public array $data)
    {
    }
}
