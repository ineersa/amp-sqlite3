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
 * Bounded idle cache of native statements prepared for direct query/execute calls.
 *
 * Exact SQL bytes are the key. Only idle handles are retained; active results keep ownership
 * until they finalize. Eviction closes the least recently used idle entry.
 *
 * @internal
 */
final class ImplicitStatementCache
{
    /** @var array<string, \SQLite3Stmt> */
    private array $idle = [];

    public function __construct(
        private readonly int $capacity,
    ) {
    }

    public function borrow(string $sql): ?\SQLite3Stmt
    {
        if (!isset($this->idle[$sql])) {
            return null;
        }

        $statement = $this->idle[$sql];
        unset($this->idle[$sql]);

        return $statement;
    }

    public function put(string $sql, \SQLite3Stmt $statement): void
    {
        if ($this->capacity === 0) {
            $statement->close();

            return;
        }

        if (isset($this->idle[$sql])) {
            $this->idle[$sql]->close();
            unset($this->idle[$sql]);
        }

        while (\count($this->idle) >= $this->capacity) {
            $oldest = \array_key_first($this->idle);
            if ($oldest === null) {
                break;
            }
            $this->idle[$oldest]->close();
            unset($this->idle[$oldest]);
        }

        $this->idle[$sql] = $statement;
    }

    public function flush(): void
    {
        foreach ($this->idle as $statement) {
            $statement->close();
        }
        $this->idle = [];
    }

    public function count(): int
    {
        return \count($this->idle);
    }
}
