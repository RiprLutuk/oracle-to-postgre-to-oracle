# oracle-pg-sync-audit

[![CI](https://github.com/RiprLutuk/oracle-to-postgre-to-oracle/actions/workflows/ci.yml/badge.svg)](https://github.com/RiprLutuk/oracle-to-postgre-to-oracle/actions/workflows/ci.yml)

Project ini menyatukan audit metadata, compare rowcount, sync data Oracle ke PostgreSQL, sync reverse PostgreSQL ke Oracle, dan reporting DBA dalam satu CLI modular.

## Guide Lengkap

- [Quick Start](docs/USER_GUIDE.md): setup awal, install, isi `.env`, isi `config.yaml`, dan command harian.
- [Configuration Reference](docs/CONFIG_REFERENCE.md): penjelasan semua field `.env` dan `config.yaml`.
- [Production Runbook](docs/PRODUCTION_RUNBOOK.md): alur audit, dry-run, eksekusi, validasi, rollback, dan checklist produksi.
- [Production Safety Features](docs/PRODUCTION_FEATURES.md): checkpoint/resume, incremental sync, checksum validation, LOB strategy, run manifest, dan CI.
- [Report Reference](docs/REPORT_REFERENCE.md): arti setiap file report dan cara membaca status `MATCH`, `WARNING`, `MISMATCH`, `MISSING`.
- [Troubleshooting](docs/TROUBLESHOOTING.md): error umum Oracle, PostgreSQL, dependency, rowcount, dan sync.
- [Oracle Client Install](docs/ORACLE_CLIENT_INSTALL.md): cara install Oracle Instant Client 23.9 untuk thick mode.
- [Developer Guide](docs/DEVELOPER_GUIDE.md): struktur kode, test, dan cara menambah fitur.

## Tujuan

- Sync data dari Oracle ke PostgreSQL.
- Sync data reverse dari PostgreSQL ke Oracle.
- Membuat inventory report per table.
- Membandingkan struktur kolom, tipe data, rowcount, dan dependency object.
- Menaruh semua output di folder `reports/`.
- Menjaga safety: `sync` default dry-run, action destructive harus eksplisit dengan `--execute`.

## Struktur

```text
oracle-pg-sync-audit/
  README.md
  requirements.txt
  .env.example
  config.yaml.example
  oracle_pg_sync/
    cli.py
    config.py
    db/
    metadata/
    sync/
    reports/
    utils/
  configs/
  reports/
  tests/
```

## Install

```bash
cd oracle-pg-sync-audit
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Atau install sebagai package lokal supaya command `oracle-pg-sync-audit` tersedia:

```bash
pip install -e ".[dev]"
```

Jika memakai Oracle Instant Client thick mode, ikuti [Oracle Client Install](docs/ORACLE_CLIENT_INSTALL.md), lalu isi `ORACLE_CLIENT_LIB_DIR` di `.env`.

## Setup Config

```bash
cp .env.example .env
cp config.yaml.example config.yaml
```

Isi koneksi di `.env`. Password tidak hardcode di YAML, cukup pakai placeholder seperti `${ORACLE_PASSWORD}` dan `${PG_PASSWORD}`.

Contoh table config:

```yaml
tables:
  - name: public.sample_customer
    mode: truncate
    key_columns: [customer_id]
  - name: public.sample_order
    mode: truncate
    key_columns: [order_id]
```

Rename column Oracle ke PostgreSQL:

```yaml
rename_columns:
  public.sample_customer:
    legacy_status: status
```

Daftar `tables` real disimpan di file lokal `config.yaml` atau `configs/tables.yaml` dan tidak ikut Git. File `*.example` berisi dummy supaya aman dipublish.

## Command

Audit metadata, rowcount, dependency:

```bash
python -m oracle_pg_sync audit --config config.yaml
python -m oracle_pg_sync audit --config config.yaml --tables sample_customer sample_order
python -m oracle_pg_sync audit --config config.yaml --all-postgres-tables --fast-count
python -m oracle_pg_sync audit-objects --config config.yaml
python -m oracle_pg_sync audit --config config.yaml --suggest-drop --sql-out reports/schema_suggestions.sql
```

Kalau `tables` kosong, command `audit` otomatis mengambil semua table dari PostgreSQL schema di config, sama seperti script `example/verify_oracle_pg.py` lama. Jika config masih punya table list tapi ingin compare semua table PostgreSQL, gunakan `--all-postgres-tables`. Hasil audit juga membuat `reports/schema_suggestions.sql` berisi saran `ALTER TABLE ADD COLUMN`; opsi `--suggest-drop` menambahkan saran `DROP COLUMN` untuk kolom yang hanya ada di PostgreSQL.

Sync Oracle ke PostgreSQL dry-run, default aman. Default mode sekarang `truncate` supaya index, trigger, grants, view/materialized view dependency tetap nempel ke table yang sama dan tidak membuat staging table besar:

```bash
python -m oracle_pg_sync sync --config config.yaml --direction oracle-to-postgres --tables sample_customer
python -m oracle_pg_sync sync --config config.yaml --direction oracle-to-postgres --tables-file configs/tables.yaml --limit 10
python -m oracle_pg_sync sync --config config.yaml --direction oracle-to-postgres --tables-file configs/tables.yaml --incremental
```

Sync PostgreSQL ke Oracle dry-run:

```bash
python -m oracle_pg_sync sync --config config.yaml --direction postgres-to-oracle --tables sample_customer --mode truncate
```

Eksekusi sync sungguhan:

```bash
python -m oracle_pg_sync sync --config config.yaml --direction oracle-to-postgres --tables sample_customer --execute
python -m oracle_pg_sync sync --config config.yaml --direction postgres-to-oracle --tables sample_customer --mode truncate --execute
```

Checkpoint/resume dan watermark:

```bash
python -m oracle_pg_sync sync --config config.yaml --list-runs
python -m oracle_pg_sync sync --config config.yaml --resume RUN_ID --execute
python -m oracle_pg_sync sync --config config.yaml --reset-checkpoint RUN_ID
python -m oracle_pg_sync sync --config config.yaml --watermark-status
python -m oracle_pg_sync sync --config config.yaml --reset-watermark public.sample_customer
```

Generate ulang HTML dari CSV:

```bash
python -m oracle_pg_sync report --config config.yaml
```

Audit, sync, audit ulang, report:

```bash
python -m oracle_pg_sync all --config config.yaml --execute
```

Jika sudah install editable:

```bash
oracle-pg-sync-audit audit --config config.yaml
```

## Output Report

Semua output masuk ke `reports/`:

- `inventory_summary.csv`
- `inventory_summary.xlsx`
- `column_diff.csv`
- `type_mismatch.csv`
- `object_dependency_summary.csv`
- `sync_result.csv`
- `sync.log`
- `report.html`
- `run_<timestamp>_<run_id>/manifest.json`

`report.html` menampilkan total table, jumlah `MATCH`, `WARNING`, `MISMATCH`, `MISSING`, top table rowcount terbesar, column mismatch, rowcount mismatch, dependency terbesar, dan table yang gagal sync.

## Mode Sync

- `truncate`: truncate target lalu load ulang. Ini default untuk menjaga object table existing seperti index, trigger, grants, view/materialized view dependency.
- `swap`: create `__load`, copy data, verify rowcount, lalu rename staging menjadi live table. Tidak default dan execute di-guard oleh `allow_swap` karena bisa membuat storage/temp RDS penuh dan dependency by OID bisa terdampak.
- `append`: insert data tanpa hapus data lama.
- `upsert`: load ke staging lalu `INSERT ... ON CONFLICT`, wajib isi `key_columns`.
- `delete`: khusus PostgreSQL ke Oracle, `DELETE` target lalu insert ulang dalam transaction.

Oracle ke PostgreSQL memakai PostgreSQL `COPY FROM STDIN`. PostgreSQL ke Oracle memakai batch `executemany` dan Oracle `MERGE` untuk upsert.

## Safety Production

- `sync` tidak mengubah data kecuali diberi `--execute`.
- Setiap audit/sync/all membuat run manifest tanpa password.
- Checkpoint SQLite disimpan di `reports/checkpoints/` dan dapat dipakai untuk `--resume RUN_ID`.
- Incremental sync memakai watermark tersimpan dan hanya mengupdate watermark setelah sync sukses.
- Checksum validation dapat diaktifkan untuk mendeteksi mismatch data selain rowcount.
- LOB sync default `error`; pilih `skip`, `null`, atau `stream` secara eksplisit.
- Default `parallel_workers: 1`, `fast_count: true`, dan `exact_count_after_load: false` supaya tidak terlalu berat di client/server.
- PostgreSQL `pg_lock_timeout: 5s` membuat sync gagal cepat jika table sedang terkunci, bukan menunggu lock lama.
- Jika struktur mismatch fatal, table di-skip kecuali pakai `--force`.
- `swap` dinonaktifkan untuk execute kecuali `sync.allow_swap: true` atau command memakai `--force`.
- `swap` memakai estimasi `pg_total_relation_size` dan `max_swap_table_bytes` untuk mencegah staging table besar jalan tanpa sadar.
- `keep_old_after_swap: false` direkomendasikan di RDS agar storage cepat balik setelah swap selesai.
- Jangan aktifkan `truncate_cascade` tanpa approval DBA.
- Exact count (`--exact-count`) memakai `SELECT COUNT(1)` dan bisa berat di table besar.
- Untuk table besar, gunakan `fast_count: true` saat audit dan jalankan exact verification hanya saat window maintenance.
- Log tidak mencetak password.

## Known Limitation

- Dependency rebuild untuk view/materialized view kompleks belum otomatis.
- Partitioned table dan LOB sangat besar mungkin butuh tuning batch/chunk tambahan.
- Incremental `oracle_scn` baru tersedia sebagai interface/config guard; implementasi Flashback/SCN akan gagal jelas sampai diaktifkan.
- PostgreSQL function/procedure body dependency ke table tidak selalu tersedia di `pg_depend`; gunakan global object compare untuk validasi object existence.
- Upsert membutuhkan unique index/constraint di PostgreSQL sesuai `key_columns`.
- Type compatibility bersifat fuzzy untuk audit; keputusan final perubahan DDL tetap harus direview DBA.

## Development Check

Test unit yang tidak butuh koneksi database:

```bash
PYTHONPATH=. python -m unittest discover -s tests
```

Untuk panduan operasional detail, mulai dari [Quick Start](docs/USER_GUIDE.md).
