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

/** Captures SQLite status before cleanup changes the connection's last error. @internal */
final class WorkerOperationException extends \RuntimeException
{
    /**
     * Null native codes identify application validation or non-native failures.
     * An empty cleanup list means the primary failure did not invalidate ownership.
     * @param list<self> $cleanupFailures
     */
    public function __construct(
        public readonly string $query,
        public readonly ?int $primaryCode,
        public readonly ?int $extendedCode,
        \Throwable $previous,
        public readonly array $cleanupFailures = [],
    ) {
        parent::__construct($previous->getMessage(), previous: $previous);
    }
}
