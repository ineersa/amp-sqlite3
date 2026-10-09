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

namespace Fabpot\Amp\Sqlite\Internal;

/**
 * Typed transaction-control actions for the private worker protocol.
 *
 * @internal
 */
enum SqliteTransactionControlAction: string
{
    case Begin = 'begin';
    case Commit = 'commit';
    case Rollback = 'rollback';
    case Savepoint = 'savepoint';
    case ReleaseSavepoint = 'release-savepoint';
    case RollbackToSavepoint = 'rollback-to-savepoint';
}
