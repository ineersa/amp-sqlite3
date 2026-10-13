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
use Fabpot\Amp\Sqlite\SqliteException;
use Fabpot\Amp\Sqlite\SqliteQueryError;

/**
 * Checks the responses sent by the SQLite child process.
 *
 * @internal
 */
final class WorkerResponse
{
    /**
     * @throws SqliteQueryError If SQLite rejected the SQL
     * @throws SqliteException  If another SQLite operation failed
     * @throws WorkerFailure    If the response is invalid or reports a protocol error
     */
    public static function unwrap(mixed $response, int $id, string $sql): mixed
    {
        if (!\is_array($response) || \count($response) !== 2 || ($response['id'] ?? null) !== $id) {
            self::invalid();
        }

        if (\array_key_exists('protocol_error', $response)) {
            $error = $response['protocol_error'];
            if (!\is_array($error) || \count($error) !== 1 || !\is_string($error['message'] ?? null)) {
                self::invalid();
            }

            throw new WorkerFailure($error['message']);
        }

        if (\array_key_exists('query_error', $response)) {
            $error = $response['query_error'];
            if (!\is_array($error)
                || \count($error) !== (\array_key_exists('query', $error) ? 4 : 3) + (\array_key_exists('cleanup_errors', $error) ? 1 : 0)
                || !\is_string($error['message'] ?? null)
                || !\array_key_exists('code', $error)
                || ($error['code'] !== null && !\is_int($error['code']))
                || !\array_key_exists('extended_code', $error)
                || ($error['extended_code'] !== null && !\is_int($error['extended_code']))
            ) {
                self::invalid();
            }

            if (\array_key_exists('query', $error)) {
                if (!\is_string($error['query'])) {
                    self::invalid();
                }
                $sql = $error['query'];
            }

            if (\array_key_exists('cleanup_errors', $error)) {
                $diagnostics = $error['cleanup_errors'];
                if (!\is_array($diagnostics) || !\array_is_list($diagnostics) || $diagnostics === []) {
                    self::invalid();
                }
                // Null marks the end of the secondary diagnostic chain.
                $previous = null;
                foreach (\array_reverse($diagnostics) as $diagnostic) {
                    if (!\is_array($diagnostic)
                        || \count($diagnostic) !== 4
                        || !\is_string($diagnostic['message'] ?? null)
                        || !\is_string($diagnostic['query'] ?? null)
                        || !\array_key_exists('code', $diagnostic)
                        || ($diagnostic['code'] !== null && !\is_int($diagnostic['code']))
                        || !\array_key_exists('extended_code', $diagnostic)
                        || ($diagnostic['extended_code'] !== null && !\is_int($diagnostic['extended_code']))
                    ) {
                        self::invalid();
                    }
                    $previous = new SqliteQueryError($diagnostic['message'], $diagnostic['query'], $diagnostic['code'], $diagnostic['extended_code'], $previous);
                }
                throw new UnusableOperationException($error['message'], $sql, $error['code'], $error['extended_code'], $previous);
            }

            throw new SqliteQueryError($error['message'], $sql, $error['code'], $error['extended_code']);
        }

        if (\array_key_exists('operation_error', $response)) {
            $error = $response['operation_error'];
            if (!\is_array($error)
                || \count($error) !== 2
                || !\is_string($error['message'] ?? null)
                || !\array_key_exists('code', $error)
                || ($error['code'] !== null && !\is_int($error['code']))
            ) {
                self::invalid();
            }

            throw new SqliteException($error['message'], $error['code'] ?? 0);
        }

        if (!\array_key_exists('value', $response)) {
            self::invalid();
        }

        return $response['value'];
    }

    public static function operation(mixed $value, int $maximumBytes, \Fabpot\Amp\Sqlite\Worker\Mode $mode): \Fabpot\Amp\Sqlite\Worker\DTO\OperationResultDTO
    {
        if (!\is_array($value) || \count($value) !== 2) {
            self::invalid();
        }
        if (!\is_array($value['data'] ?? null)) {
            self::invalid();
        }
        if (!\is_string($value['outcome'] ?? null)) {
            self::invalid();
        }
        $outcome = \Fabpot\Amp\Sqlite\Worker\Outcome::tryFrom($value['outcome']);
        if ($outcome === null) {
            self::invalid();
        }
        if ($mode === \Fabpot\Amp\Sqlite\Worker\Mode::ReadOnly && $outcome !== \Fabpot\Amp\Sqlite\Worker\Outcome::ReadOnly) {
            self::invalid();
        }
        if ($mode === \Fabpot\Amp\Sqlite\Worker\Mode::Transaction && $outcome === \Fabpot\Amp\Sqlite\Worker\Outcome::ReadOnly) {
            self::invalid();
        }
        try {
            OperationValues::bound($value, $maximumBytes);
        } catch (\Throwable $exception) {
            throw new WorkerFailure('Invalid SQLite worker operation result', previous: $exception);
        }

        return new \Fabpot\Amp\Sqlite\Worker\DTO\OperationResultDTO($outcome, $value['data']);
    }

    public static function void(mixed $value): void
    {
        if ($value !== null) {
            self::invalid();
        }
    }

    /**
     * @return SqliteResultPayload
     */
    public static function result(mixed $value): array
    {
        if (!\is_array($value)
            || \count($value) !== 7
            || !\array_key_exists('result_id', $value)
            || ($value['result_id'] !== null && !\is_int($value['result_id']))
            || !self::isRowList($value['rows'] ?? null)
            || !\is_bool($value['exhausted'] ?? null)
            || !\array_key_exists('row_count', $value)
            || ($value['row_count'] !== null && !\is_int($value['row_count']))
            || !\array_key_exists('column_count', $value)
            || ($value['column_count'] !== null && !\is_int($value['column_count']))
            || !\array_key_exists('column_names', $value)
            || !self::isStringListOrNull($value['column_names'])
            || !\array_key_exists('last_insert_id', $value)
            || ($value['last_insert_id'] !== null && !\is_int($value['last_insert_id']))
        ) {
            self::invalid();
        }

        /** @var list<string>|null $columnNames */
        $columnNames = $value['column_names'];
        if ($value['result_id'] === null) {
            if ($value['rows'] !== []
                || !$value['exhausted']
                || $value['row_count'] === null
                || $value['row_count'] < 0
                || $value['column_count'] !== null
                || $columnNames !== null
            ) {
                self::invalid();
            }
        } elseif ($value['result_id'] < 1
            || $value['row_count'] !== null
            || $value['column_count'] === null
            || $value['column_count'] < 1
            || $columnNames === null
            || \count($columnNames) !== $value['column_count']
        ) {
            self::invalid();
        }

        /** @var SqliteResultPayload $value */
        return $value;
    }

    /**
     * @return SqliteBatchPayload
     */
    public static function batch(mixed $value): array
    {
        if (!\is_array($value) || \count($value) !== 2 || !self::isRowList($value['rows'] ?? null) || !\is_bool($value['exhausted'] ?? null)) {
            self::invalid();
        }

        /** @var SqliteBatchPayload $value */
        if (!$value['exhausted'] && $value['rows'] === []) {
            self::invalid();
        }

        return $value;
    }

    /**
     * @return array{blob_id: int, length: int}
     */
    public static function openBlob(mixed $value): array
    {
        if (!\is_array($value) || \count($value) !== 2 || !\is_int($value['blob_id'] ?? null) || $value['blob_id'] < 1 || !\is_int($value['length'] ?? null) || $value['length'] < 0) {
            self::invalid();
        }

        /** @var array{blob_id: int, length: int} $value */
        return $value;
    }

    public static function statementId(mixed $value): int
    {
        if (!\is_array($value) || \count($value) !== 1 || !\is_int($value['statement_id'] ?? null) || $value['statement_id'] < 1) {
            self::invalid();
        }

        return $value['statement_id'];
    }

    public static function blobBytes(mixed $value): string
    {
        if (!\is_array($value) || \count($value) !== 1 || !\is_string($value['bytes'] ?? null)) {
            self::invalid();
        }

        return $value['bytes'];
    }

    private static function isRowList(mixed $value): bool
    {
        if (!\is_array($value) || !\array_is_list($value)) {
            return false;
        }

        foreach ($value as $row) {
            if (!\is_array($row) || !\array_all($row, self::isRowValue(...))) {
                return false;
            }
        }

        return true;
    }

    private static function isRowValue(mixed $value): bool
    {
        return $value === null
            || \is_int($value)
            || \is_float($value)
            || \is_string($value)
            || $value instanceof SqliteBlob;
    }

    private static function isStringListOrNull(mixed $value): bool
    {
        if ($value === null) {
            return true;
        }
        if (!\is_array($value) || !\array_is_list($value)) {
            return false;
        }

        foreach ($value as $item) {
            if (!\is_string($item)) {
                return false;
            }
        }

        return true;
    }

    private static function invalid(): never
    {
        throw new WorkerFailure('Received an invalid response from the SQLite child process');
    }
}
