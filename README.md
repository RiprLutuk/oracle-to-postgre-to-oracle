# oracle-pg-sync-audit

[![CI]][ci-workflow]

[CI]: https://github.com/RiprLutuk/oracle-pg-sync-audit/actions/workflows/ci.yml/badge.svg
[ci-workflow]: https://github.com/RiprLutuk/oracle-pg-sync-audit/actions/workflows/ci.yml

Project ini menyatukan audit metadata, compare rowcount, sync data Oracle ke
PostgreSQL, sync reverse PostgreSQL ke Oracle, dan reporting DBA dalam satu CLI
modular.

## Guide Lengkap

- [Quick Start](docs/USER_GUIDE.md): setup awal, install, isi `.env`, isi `config.yaml`, dan command harian.
- [Configuration Reference](docs/CONFIG_REFERENCE.md): penjelasan semua field `.env` dan `config.yaml`.
- [Production Runbook](docs/PRODUCTION_RUNBOOK.md): alur audit, dry-run, eksekusi, validasi, rollback, dan checklist produksi.
- [Production Safety Features](docs/PRODUCTION_FEATURES.md): checkpoint/resume,
  incremental sync, checksum validation, LOB strategy, run manifest, dan CI.
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

Jika memakai Oracle Instant Client thick mode, ikuti
[Oracle Client Install](docs/ORACLE_CLIENT_INSTALL.md), lalu isi
`ORACLE_CLIENT_LIB_DIR` di `.env`.

## Setup Config

```bash
cp .env.example .env
cp config.yaml.example config.yaml
```

Isi koneksi di `.env`. Password tidak hardcode di YAML, cukup pakai placeholder seperti `${ORACLE_PASSWORD}` dan `${PG_PASSWORD}`.

Main config menunjuk table list terpisah:

```yaml
tables_file: configs/tables.yaml
```

Isi `configs/tables.yaml` cukup daftar table supaya mudah dibaca:

```yaml
tables:
  - public.address
  - public.housemaster
  - public.a_hp_house_info
```

Detail run yang berubah-ubah seperti `where`, `key_columns`, dan incremental column lebih aman ditaruh di command/job script.

Rename column Oracle ke PostgreSQL:

```yaml
rename_columns:
  public.sample_customer:
    legacy_status: status
```

Daftar `tables` real disimpan di satu tempat: `configs/tables.yaml`.
`config.yaml` cukup memakai `tables_file: configs/tables.yaml` untuk
menghindari dua table list yang bisa berbeda.

## Command

Audit metadata, rowcount, dependency:

```bash
python -m oracle_pg_sync audit --config config.yaml
python -m oracle_pg_sync audit --config config.yaml --tables sample_customer sample_order
python -m oracle_pg_sync audit --config config.yaml --all-postgres-tables --fast-count
python -m oracle_pg_sync audit-objects --config config.yaml
python -m oracle_pg_sync audit --config config.yaml --suggest-drop
```

Kalau `tables` kosong, command `audit` otomatis mengambil semua table dari
PostgreSQL schema di config, sama seperti script `example/verify_oracle_pg.py`
lama. Jika config masih punya table list tapi ingin compare semua table
PostgreSQL, gunakan `--all-postgres-tables`. Hasil audit juga membuat
`schema_suggestions.sql` di folder run berisi saran `ALTER TABLE ADD COLUMN`;
opsi `--suggest-drop` menambahkan saran `DROP COLUMN` untuk kolom yang hanya
ada di PostgreSQL.

Sync Oracle ke PostgreSQL dry-run, default aman. Default mode sekarang
`truncate` supaya index, trigger, grants, view/materialized view dependency
tetap nempel ke table yang sama dan tidak membuat staging table besar:

```bash
python -m oracle_pg_sync sync --config config.yaml --direction oracle-to-postgres --tables sample_customer
python -m oracle_pg_sync sync --config config.yaml --direction oracle-to-postgres --tables-file configs/tables.yaml --limit 10
python -m oracle_pg_sync sync --config config.yaml --direction oracle-to-postgres --tables-file configs/tables.yaml --incremental
```

Sync PostgreSQL ke Oracle dry-run:

```bash
python -m oracle_pg_sync sync --config config.yaml --direction postgres-to-oracle --tables sample_customer --mode truncate
python -m oracle_pg_sync sync --config config.yaml \
  --direction postgres-to-oracle \
  --tables public.address \
  --mode upsert \
  --key-columns address_id \
  --incremental-column last_update \
  --incremental
```

Eksekusi sync sungguhan:

```bash
python -m oracle_pg_sync sync --config config.yaml --direction oracle-to-postgres --tables sample_customer --execute
python -m oracle_pg_sync sync --config config.yaml --direction postgres-to-oracle --tables sample_customer --mode truncate --execute
ops sync --go \
  --direction postgres-to-oracle \
  --tables public.address \
  --mode upsert \
  --key-columns address_id \
  --incremental-column last_update \
  --where "last_update >= CURRENT_TIMESTAMP - INTERVAL '5 minutes'" \
  --incremental
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

DBA-friendly alias:

```bash
ops audit
ops sync
ops sync --go
ops resume
ops status
ops report latest
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

Setiap eksekusi membuat satu folder run yang lengkap:

```text
reports/
  run_<timestamp>_<run_id>/
    manifest.json
    report.xlsx
    report.html
    logs.txt
    inventory_summary.csv
    sync_result.csv
    validation_checksum.csv
    dependency_pre.csv
    dependency_post.csv
    dependency_maintenance.csv
```

File yang tidak relevan untuk command tertentu tidak dibuat. Root `reports/` dipakai untuk checkpoint, lock, dan log runtime global saja.

Central Excel `report.xlsx` berisi sheet:

- `00_Dashboard`
- `01_Run_Summary`
- `02_Table_Sync_Status`
- `03_Rowcount_Compare`
- `04_Checksum_Result`
- `05_Column_Diff`
- `06_Index_Compare`
- `07_Object_Dependency`
- `08_LOB_Columns`
- `09_Failed_Tables`
- `10_Watermark`
- `11_Checkpoint`
- `12_Performance`
- `13_Errors`
- `14_Config`

`report.html` menampilkan total table, jumlah `MATCH`, `WARNING`, `MISMATCH`,
`MISSING`, top table rowcount terbesar, column mismatch, rowcount mismatch,
dependency terbesar, checksum mismatch, LOB summary, dan table yang gagal sync.

## Mode Sync

- `truncate`: truncate target lalu load ulang. Ini default untuk menjaga object table existing.
- `swap`: create `__load`, copy data, verify rowcount, lalu rename staging menjadi live table.
  Tidak default dan execute di-guard oleh `allow_swap` karena bisa membuat storage/temp RDS penuh.
- `append`: insert data tanpa hapus data lama.
- `upsert`: load ke staging lalu `INSERT ... ON CONFLICT`, wajib isi `key_columns`.
- `delete`: khusus PostgreSQL ke Oracle, `DELETE` target lalu insert ulang dalam transaction.

Oracle ke PostgreSQL memakai PostgreSQL `COPY FROM STDIN`. PostgreSQL ke Oracle memakai batch `executemany` dan Oracle `MERGE` untuk upsert.
Untuk reverse upsert, `key_columns` bisa berasal dari config atau command `--key-columns`.

## Safety Production

- `sync` tidak mengubah data kecuali diberi `--execute`.
- Setiap audit/sync/all membuat run manifest tanpa password.
- Checkpoint SQLite disimpan di `reports/checkpoints/` dan dapat dipakai untuk `--resume RUN_ID`.
- Incremental sync memakai watermark tersimpan dan hanya mengupdate watermark setelah sync sukses.
- Checksum validation dapat diaktifkan untuk mendeteksi mismatch data selain rowcount.
- LOB sync default `error`; pilih `skip`, `null`, `stream`, atau `include` secara eksplisit.
  Oracle `BLOB`, `CLOB`, `NCLOB`, `LONG`, dan `LONG RAW` terdeteksi end-to-end.
- DBA shortcut CLI tersedia sebagai `ops`, misalnya `ops sync --go --lob stream`,
  `ops doctor`, `ops dependencies check`, `ops dependencies repair`,
  `ops analyze lob`, dan `ops resume RUN_ID`.
- Sync membuat dependency report sebelum dan sesudah load: `dependency_pre.csv` dan `dependency_post.csv`.
- Saat execute, toolkit mencoba Oracle invalid object compile dan PostgreSQL MV refresh/validation.
- Scheduler pack tersedia di `jobs/daily.sh` dan `jobs/every_5min.sh`; keduanya memakai `--profile`, lock file, dan log rotation.
- Cron template tersedia di `jobs/crontab.example`. Set `ALERT_COMMAND` untuk menerima alert saat job keluar non-zero.
- Default `parallel_workers: 1`, `fast_count: true`, dan `exact_count_after_load: false` supaya tidak terlalu berat di client/server.
- PostgreSQL `pg_lock_timeout: 5s` membuat sync gagal cepat jika table sedang terkunci, bukan menunggu lock lama.
- Jika struktur mismatch fatal, table di-skip kecuali pakai `--force`.
- `swap` dinonaktifkan untuk execute kecuali `sync.allow_swap: true` atau command memakai `--force`.
- `swap` memakai estimasi `pg_total_relation_size` dan `max_swap_table_bytes` untuk mencegah staging table besar jalan tanpa sadar.
- `keep_old_after_swap: false` direkomendasikan di RDS agar storage cepat balik setelah swap selesai.
- Jangan aktifkan `truncate_cascade` tanpa approval DBA.
- Exact count (`--exact-count`) memakai `SELECT COUNT(1)` dan bisa berat di table besar.
- Untuk table besar, gunakan `fast_count: true` saat audit dan jalankan exact verification hanya saat window maintenance.
- Audit type compatibility mengenali alias umum Oracle/PostgreSQL, termasuk
  `NUMBER`/`NUMERIC`, `VARCHAR2`/`varchar`, `CLOB`/`text`, `BLOB`/`bytea`,
  `DATE`/`timestamp`, `INTERVAL`/`interval`, `BOOLEAN`/`boolean`, `ROWID`/`text`,
  dan `JSON`/`jsonb`.
- Log tidak mencetak password.

## Known Limitation

- Dependency rebuild untuk view/materialized view kompleks belum otomatis.
- Partitioned table dan LOB sangat besar mungkin butuh tuning batch/chunk tambahan.
- Incremental `oracle_scn` baru tersedia sebagai interface/config guard; implementasi Flashback/SCN akan gagal jelas sampai diaktifkan.
- PostgreSQL function/procedure body dependency ke table tidak selalu tersedia di `pg_depend`.
- Upsert membutuhkan unique index/constraint di PostgreSQL sesuai `key_columns`.
- Type compatibility bersifat fuzzy untuk audit; keputusan final perubahan DDL tetap harus direview DBA.

## Development Check

Test unit yang tidak butuh koneksi database:

```bash
PYTHONPATH=. python -m unittest discover -s tests
```

Validasi command setelah install editable:

```bash
pip install -e ".[dev]"
oracle-pg-sync-audit --help
ops report latest --config config.yaml.example
```

Integration check opsional untuk PostgreSQL container + fake Oracle MERGE:

```bash
RUN_CONTAINER_TESTS=1 python tests/integration_reverse_merge_container.py
```

Untuk panduan operasional detail, mulai dari [Quick Start](docs/USER_GUIDE.md).
