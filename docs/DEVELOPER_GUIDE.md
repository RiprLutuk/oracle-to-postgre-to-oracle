# Developer Guide

Panduan ini untuk developer yang ingin mengubah atau memperluas project.

## Struktur Modul

```text
oracle_pg_sync/
  cli.py
  config.py
  db/
    oracle.py
    postgres.py
  metadata/
    oracle_metadata.py
    postgres_metadata.py
    compare.py
    type_mapping.py
  sync/
    oracle_to_postgres.py
    postgres_to_oracle.py
    staging.py
    copy_loader.py
    verifier.py
  reports/
    writer_csv.py
    writer_excel.py
    writer_html.py
  utils/
```

## Entry Point

CLI utama:

```text
oracle_pg_sync/cli.py
```

Module execution:

```bash
python -m oracle_pg_sync audit --config config.yaml
```

Installed script:

```bash
oracle-pg-sync-audit audit --config config.yaml
```

## Config Flow

1. `load_config()` membaca YAML/JSON.
2. `env_file` diload jika ada.
3. Placeholder `${VAR}` dan `${VAR:-default}` diexpand.
4. Config diubah menjadi dataclass `AppConfig`.

File:

```text
oracle_pg_sync/config.py
```

## Metadata Flow

Oracle:

```text
metadata/oracle_metadata.py
db/oracle.py
```

PostgreSQL:

```text
metadata/postgres_metadata.py
db/postgres.py
```

Compare:

```text
metadata/compare.py
metadata/type_mapping.py
```

## Sync Flow

Class utama:

```text
sync/oracle_to_postgres.py
sync/postgres_to_oracle.py
```

Flow per table:

1. Fetch metadata Oracle dan PostgreSQL.
2. Compare struktur.
3. Skip jika mismatch fatal dan tidak `--force`.
4. Build column mapping.
5. Jalankan mode sync.
6. Verify rowcount jika aktif.
7. Tulis `sync_result.csv`.

## Menambah Type Mapping

Edit:

```text
metadata/type_mapping.py
```

Fungsi penting:

- `is_type_compatible()`
- `suggested_pg_type()`
- `oracle_type_label()`
- `pg_type_label()`

Tambahkan unit test di:

```text
tests/test_type_mapping.py
```

## Menambah Field Report

1. Tambahkan field di `metadata/compare.py`.
2. Pastikan writer CSV otomatis menangkap field baru.
3. Jika perlu tampil di HTML, edit `reports/writer_html.py`.
4. Update `docs/REPORT_REFERENCE.md`.

## Menambah Mode Sync

1. Tambahkan pilihan CLI di `cli.py`.
2. Tambahkan branch di `OracleToPostgresSync.sync_table()`.
3. Implement method baru di `sync/oracle_to_postgres.py`.
4. Tambahkan test unit jika bisa tanpa DB, atau integration test terpisah.

## Direction Sync

CLI mendukung:

- `--direction oracle-to-postgres`
- `--direction postgres-to-oracle`

Resolver runner ada di:

```text
oracle_pg_sync/cli.py
```

## Test

Unit test tanpa koneksi DB:

```bash
PYTHONPATH=. python -m unittest discover -s tests
```

Jika dependency dev terinstall:

```bash
pytest
```

Compile check:

```bash
python -m compileall oracle_pg_sync tests
```

CI GitHub Actions menjalankan install dependency, compile check, unit test, parse `config.yaml.example`, dan basic committed-secret checks. Test database production harus tetap memakai mock/stub atau SQLite lokal, bukan credential Oracle/PostgreSQL real.

## Style

- Python 3.11+.
- Type hints untuk fungsi baru.
- Jangan hardcode password.
- Jangan print credential ke log.
- Keep destructive action explicit.
- Tambah test untuk logic compare/type/config.
