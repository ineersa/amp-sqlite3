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

use Fabpot\Amp\Sqlite\SqliteBlob;

/** Validates data before serialization and enforces a serialized result-size limit. @internal */
final class OperationValues
{
    private const MAX_DEPTH = 32;

    /** @param array<array-key, mixed> $values */
    public static function validate(array $values): void
    {
        self::check($values, 0);
    }

    /** @param array<array-key, mixed> $values */
    public static function bound(array $values, int $maximumBytes): void
    {
        self::validate($values);
        if (\strlen(\serialize($values)) > $maximumBytes) {
            throw new \LengthException('Worker operation result exceeds its byte limit');
        }
    }

    private static function check(mixed $value, int $depth): void
    {
        if ($depth > self::MAX_DEPTH) {
            throw new \InvalidArgumentException('Worker operation data exceeds its nesting limit');
        }
        if (\is_array($value)) {
            foreach ($value as $item) {
                self::check($item, $depth + 1);
            }
            return;
        }
        if ($value !== null && !\is_scalar($value) && !$value instanceof SqliteBlob) {
            throw new \InvalidArgumentException('Worker operation data must contain only scalar values, arrays, or SQLite BLOBs');
        }
    }
}
