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

use Amp\Cancellation;

/**
 * A raw SQLite connection that can tighten its graceful close budget.
 *
 * Ordinary {@see SqliteConnection::close()} remains bounded. This interface exposes an
 * optional cancellation for callers that already own a tighter deadline. Connection pools
 * do not implement it.
 */
interface SqliteCancellableConnection extends SqliteConnection
{
    /**
     * Closes the connection and asks the child process to exit.
     *
     * Graceful shutdown is bounded even without a cancellation. Pass a cancellation to
     * tighten that budget; when it expires, the child is force-terminated.
     */
    public function close(?Cancellation $cancellation = null): void;
}
