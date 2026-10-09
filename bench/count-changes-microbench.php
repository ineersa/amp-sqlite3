#!/usr/bin/env php
<?php

declare(strict_types=1);

/**
 * Diagnostic microbenchmark for PRAGMA count_changes scalar reads.
 *
 * Compares the old cached prepare/execute/fetchArray helper against
 * SQLite3::querySingle. Native SQLite3 only; not an Amp end-to-end claim.
 *
 * Distinguishes PHP SQLite3::prepare call counts from SQLite automatic
 * repreparations. SQLITE_STMTSTATUS_REPREPARE is unavailable through
 * ext-sqlite3, so repreparation counts are recorded as null.
 *
 * Usage:
 *   XDEBUG_MODE=off php8.4 bench/count-changes-microbench.php
 *   XDEBUG_MODE=off php8.5 bench/count-changes-microbench.php
 */

const MIN_MEASURE_SECONDS = 0.2;
const WARMUP_ITERATIONS = 2_000;

final class CountChangesReader
{
    private ?SQLite3Stmt $cached = null;

    public function __construct(
        private readonly SQLite3 $db,
        private readonly string $mode,
    ) {
        if ($mode !== 'cached_execute_fetch' && $mode !== 'query_single') {
            throw new InvalidArgumentException('Unknown mode: '.$mode);
        }
    }

    public function read(): bool
    {
        if ($this->mode === 'query_single') {
            return (bool) $this->db->querySingle('PRAGMA count_changes');
        }

        $this->cached ??= $this->db->prepare('PRAGMA count_changes')
            ?: throw new RuntimeException('Could not prepare count_changes');
        $result = $this->cached->execute();
        if ($result === false) {
            throw new RuntimeException('Could not execute count_changes');
        }

        try {
            $row = $result->fetchArray(SQLITE3_NUM);
        } finally {
            $result->finalize();
        }

        return $row !== false && (bool) $row[0];
    }

    public function phpPrepareCalls(): int
    {
        return $this->mode === 'cached_execute_fetch' && $this->cached !== null ? 1 : 0;
    }
}

function openDatabase(): SQLite3
{
    $db = new SQLite3(':memory:');
    $db->enableExceptions(true);
    $db->busyTimeout(5_000);
    $db->exec('PRAGMA journal_mode = MEMORY');
    $db->exec('PRAGMA synchronous = OFF');
    $db->exec('PRAGMA temp_store = MEMORY');

    return $db;
}

/**
 * @return list<array{name: string, setup: Closure(SQLite3): void, body: Closure(SQLite3, CountChangesReader): void}>
 */
function pairs(): array
{
    return [
        [
            'name' => 'scalar_off',
            'setup' => static function (SQLite3 $db): void {
                $db->querySingle('PRAGMA count_changes = OFF');
            },
            'body' => static function (SQLite3 $db, CountChangesReader $reader): void {
                $reader->read();
            },
        ],
        [
            'name' => 'scalar_on',
            'setup' => static function (SQLite3 $db): void {
                $db->querySingle('PRAGMA count_changes = ON');
            },
            'body' => static function (SQLite3 $db, CountChangesReader $reader): void {
                $reader->read();
            },
        ],
        [
            'name' => 'scalar_toggle',
            'setup' => static function (SQLite3 $db): void {
                $db->querySingle('PRAGMA count_changes = OFF');
            },
            'body' => static function (SQLite3 $db, CountChangesReader $reader): void {
                $db->querySingle('PRAGMA count_changes = ON');
                $reader->read();
                $db->querySingle('PRAGMA count_changes = OFF');
                $reader->read();
            },
        ],
        [
            'name' => 'dml_insert_guard_off',
            'setup' => static function (SQLite3 $db): void {
                $db->exec('CREATE TABLE entries (id INTEGER PRIMARY KEY, value TEXT)');
                $db->querySingle('PRAGMA count_changes = OFF');
            },
            'body' => static function (SQLite3 $db, CountChangesReader $reader): void {
                if ($reader->read()) {
                    throw new RuntimeException('Unexpected count_changes ON during OFF guard path');
                }
                $db->exec("INSERT INTO entries (value) VALUES ('x')");
            },
        ],
        [
            'name' => 'dml_update_guard_off',
            'setup' => static function (SQLite3 $db): void {
                $db->exec('CREATE TABLE entries (id INTEGER PRIMARY KEY, value TEXT)');
                $db->exec("INSERT INTO entries (value) VALUES ('seed')");
                $db->querySingle('PRAGMA count_changes = OFF');
            },
            'body' => static function (SQLite3 $db, CountChangesReader $reader): void {
                if ($reader->read()) {
                    throw new RuntimeException('Unexpected count_changes ON during OFF guard path');
                }
                $db->exec("UPDATE entries SET value = 'y' WHERE id = 1");
            },
        ],
        [
            'name' => 'dml_delete_guard_off',
            'setup' => static function (SQLite3 $db): void {
                $db->exec('CREATE TABLE entries (id INTEGER PRIMARY KEY, value TEXT)');
                $db->querySingle('PRAGMA count_changes = OFF');
            },
            'body' => static function (SQLite3 $db, CountChangesReader $reader): void {
                $db->exec("INSERT INTO entries (value) VALUES ('x')");
                if ($reader->read()) {
                    throw new RuntimeException('Unexpected count_changes ON during OFF guard path');
                }
                $db->exec('DELETE FROM entries WHERE id = last_insert_rowid()');
            },
        ],
        [
            'name' => 'prepared_dml_guard_off',
            'setup' => static function (SQLite3 $db): void {
                $db->exec('CREATE TABLE entries (id INTEGER PRIMARY KEY, value TEXT)');
                $db->querySingle('PRAGMA count_changes = OFF');
            },
            'body' => static function (SQLite3 $db, CountChangesReader $reader): void {
                static $statement = null;
                static $owner = null;
                if ($owner !== $db) {
                    $statement = $db->prepare('INSERT INTO entries (value) VALUES (?)');
                    $owner = $db;
                }
                if ($reader->read()) {
                    throw new RuntimeException('Unexpected count_changes ON during OFF guard path');
                }
                $statement->bindValue(1, 'x', SQLITE3_TEXT);
                $result = $statement->execute();
                if ($result === false) {
                    throw new RuntimeException('Prepared insert failed');
                }
                $result->finalize();
            },
        ],
    ];
}

/**
 * @param Closure(SQLite3): void $setup
 * @param Closure(SQLite3, CountChangesReader): void $body
 *
 * @return array{
 *     iterations: int,
 *     seconds: float,
 *     ns_per_op: float,
 *     php_prepare_calls: int,
 *     sqlite_stmtstatus_reprepare: null,
 *     sqlite_stmtstatus_reprepare_unavailable: string
 * }
 */
function measure(string $mode, Closure $setup, Closure $body): array
{
    $db = openDatabase();
    $setup($db);
    $reader = new CountChangesReader($db, $mode);

    for ($i = 0; $i < WARMUP_ITERATIONS; ++$i) {
        $body($db, $reader);
    }

    $db->close();
    $db = openDatabase();
    $setup($db);
    $reader = new CountChangesReader($db, $mode);
    // Establish the cached helper before the timed loop so prepare cost is outside measured ops.
    $reader->read();

    $iterations = 0;
    $started = hrtime(true);
    $deadline = $started + (int) (MIN_MEASURE_SECONDS * 1_000_000_000);
    do {
        $body($db, $reader);
        ++$iterations;
    } while (hrtime(true) < $deadline || $iterations < 100);
    $seconds = (hrtime(true) - $started) / 1_000_000_000;

    $payload = [
        'iterations' => $iterations,
        'seconds' => $seconds,
        'ns_per_op' => ($seconds * 1_000_000_000) / $iterations,
        'php_prepare_calls' => $reader->phpPrepareCalls(),
        'sqlite_stmtstatus_reprepare' => null,
        'sqlite_stmtstatus_reprepare_unavailable' => 'SQLITE_STMTSTATUS_REPREPARE is not exposed by ext-sqlite3; use a native companion probe for engine repreparations',
    ];
    $db->close();

    return $payload;
}

function main(): int
{
    $rawDir = __DIR__.'/raw';
    if (!is_dir($rawDir) && !mkdir($rawDir, 0777, true) && !is_dir($rawDir)) {
        fwrite(STDERR, "Could not create {$rawDir}\n");

        return 1;
    }

    $phpVersion = PHP_VERSION;
    $sqliteVersion = SQLite3::version()['versionString'] ?? 'unknown';
    $modes = ['cached_execute_fetch', 'query_single'];
    $results = [];

    foreach (pairs() as $index => $pair) {
        $orderedModes = $index % 2 === 0 ? $modes : array_reverse($modes);
        $pairResult = [
            'name' => $pair['name'],
            'mode_order' => $orderedModes,
            'modes' => [],
        ];
        foreach ($orderedModes as $mode) {
            $pairResult['modes'][$mode] = measure($mode, $pair['setup'], $pair['body']);
        }
        $results[] = $pairResult;
    }

    $payload = [
        'benchmark' => 'count_changes_microbench',
        'kind' => 'native_sqlite3_diagnostic',
        'php_version' => $phpVersion,
        'php_sapi' => PHP_SAPI,
        'sqlite_version' => $sqliteVersion,
        'xdebug_mode' => getenv('XDEBUG_MODE') ?: null,
        'min_measure_seconds' => MIN_MEASURE_SECONDS,
        'warmup_iterations' => WARMUP_ITERATIONS,
        'settings' => [
            'journal_mode' => 'MEMORY',
            'synchronous' => 'OFF',
            'temp_store' => 'MEMORY',
            'busy_timeout_ms' => 5_000,
            'path' => ':memory:',
        ],
        'notes' => [
            'Compares cached prepare/execute/fetchArray against querySingle for count_changes only.',
            'schema_version remains on the cached path in production and is not measured here.',
            'php_prepare_calls counts SQLite3::prepare invocations owned by the helper path after warmup, not SQLite automatic repreparations.',
            'query_single uses zero PHP prepare calls; SQLite still compiles internally per querySingle.',
            'Automatic repreparation counts require SQLITE_STMTSTATUS_REPREPARE via the C API; ext-sqlite3 does not expose it.',
        ],
        'pairs' => $results,
        'completed_at' => gmdate('c'),
    ];

    $target = sprintf(
        '%s/count-changes-php%s-sqlite%s.json',
        $rawDir,
        str_replace('.', '_', $phpVersion),
        str_replace('.', '_', $sqliteVersion),
    );
    file_put_contents($target, json_encode($payload, JSON_PRETTY_PRINT | JSON_THROW_ON_ERROR)."\n");
    fwrite(STDOUT, $target."\n");

    return 0;
}

exit(main());
