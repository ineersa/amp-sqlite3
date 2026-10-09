# Driver microbenchmarks

## count_changes scalar/DML diagnostic

Compares the old cached `prepare`/`execute`/`fetchArray` path against `SQLite3::querySingle` for `PRAGMA count_changes`.

This is a native `ext-sqlite3` measurement. It is not an Amp IPC or end-to-end queue claim. Automatic SQLite repreparations are not counted here because `SQLITE_STMTSTATUS_REPREPARE` is not exposed by PHP; `php_prepare_calls` counts only `SQLite3::prepare` owned by the helper.

Run after other agents finish, to avoid host contention:

```sh
XDEBUG_MODE=off php8.4 bench/count-changes-microbench.php
XDEBUG_MODE=off php8.5 bench/count-changes-microbench.php
```

Each run writes one JSON file under `bench/raw/`.
