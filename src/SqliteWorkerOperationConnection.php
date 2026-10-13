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

namespace Fabpot\Amp\Sqlite;

use Fabpot\Amp\Sqlite\Worker\OperationExecutor;

/** Opt-in capability; ordinary SqliteConnection implementations remain unchanged. */
interface SqliteWorkerOperationConnection extends SqliteCancellableConnection
{
    /** Acquires exclusive ownership. Null limits select depth one through the same bounded coordinator. */
    public function createWorkerOperationExecutor(int $maximumResultBytes, int $maximumRows, ?\Fabpot\Amp\Sqlite\Worker\DTO\PipelineLimitsDTO $limits = null): OperationExecutor;
}
