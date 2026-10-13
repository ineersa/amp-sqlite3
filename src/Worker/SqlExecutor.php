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

namespace Fabpot\Amp\Sqlite\Worker;

/** SQL capability valid only during one worker operation; never owns a native connection. */
final class SqlExecutor
{
    private bool $active = true;

    /**
     * @param \Closure(string, array<array-key, mixed>): array<string, mixed> $execute
     * @internal Created by the worker with initialized execution and resource limits.
     */
    public function __construct(private readonly \Closure $execute)
    {
    }

    /**
     * @param array<array-key, mixed> $parameters
     * @return array<string, mixed> Fully materialized driver result fields, with no remote result ID.
     */
    public function execute(string $sql, array $parameters): array
    {
        if (!$this->active) {
            throw new \LogicException('The worker SQL executor is no longer active');
        }

        return ($this->execute)($sql, $parameters);
    }

    /** @internal The worker invalidates this capability before ending its transaction. */
    public function close(): void
    {
        $this->active = false;
    }
}
