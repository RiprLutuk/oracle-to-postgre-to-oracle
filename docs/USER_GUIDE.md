# User Guide

Dokumen ini adalah panduan utama untuk menjalankan `oracle-pg-sync-audit` dari awal sampai menghasilkan report. Tool ini mendukung dua arah sync:

- Oracle ke PostgreSQL.
- PostgreSQL ke Oracle.

## 1. Prasyarat

- Python 3.11 atau lebih baru.
- Akses network dari mesin ini ke Oracle dan PostgreSQL.
- User Oracle punya privilege baca metadata dan data table target.
- User PostgreSQL punya privilege baca metadata, insert/copy, truncate, create staging table, rename table untuk mode `swap`, dan analyze.
- Oracle Instant Client jika environment membutuhkan thick mode.

Privilege Oracle yang biasanya dibutuhkan:

```sql
SELECT_CATALOG_ROLE
SELECT ON target_schema.target_table
SELECT ON V_$DATABASE -- opsional jika nanti memakai snapshot SCN/custom logic
```

Privilege PostgreSQL yang biasanya dibutuhkan:

```sql
USAGE ON SCHEMA public
SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA public
CREATE ON SCHEMA public
```

## 2. Install

```bash
cd /home/lutuk/project/pg2ora2pg/oracle-pg-sync-audit
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Opsional, install sebagai package lokal:

```bash
pip install -e ".[dev]"
```

Setelah install editable, command ini tersedia:

```bash
oracle-pg-sync-audit --help
```

Tanpa install editable, gunakan:

```bash
python -m oracle_pg_sync --help
```

## 3. Setup File Config

```bash
cp .env.example .env
cp config.yaml.example config.yaml
```

Isi `.env` dengan credential real. Jangan commit `.env`.

Contoh minimal:

```dotenv
ORACLE_HOST=oracle-host.example.com
ORACLE_PORT=1521
ORACLE_SERVICE_NAME=ORCLPDB1
ORACLE_USER=app_reader
ORACLE_PASSWORD=REPLACE_ME
ORACLE_SCHEMA=APP_SCHEMA

PG_HOST=postgres-host.example.com
PG_PORT=5432
PG_DATABASE=target_db
PG_USER=sync_user
PG_PASSWORD=REPLACE_ME
PG_SCHEMA=public
```

Jika memakai Oracle DSN penuh, isi `ORACLE_DSN` dan host/service bisa dikosongkan:

```dotenv
ORACLE_DSN=oracle-host.example.com:1521/ORCLPDB1
```

## 4. Isi Table Target

Edit `config.yaml`:

```yaml
tables:
  - name: public.sample_customer
    oracle_to_postgres_mode: truncate
    postgres_to_oracle_mode: truncate
    directions:
      - oracle-to-postgres
      - postgres-to-oracle
    key_columns: [customer_id]
```

Nama table boleh `sample_customer` atau `public.sample_customer`. Jika schema tidak disebut, default memakai `postgres.schema`.

`config.yaml` bawaan sudah diisi dari script lama di folder `example/`:

- `config.yaml.example` dan `configs/tables.example.yaml` sengaja memakai table dummy.
- Copy table list real dari environment lokal ke `configs/tables.yaml`, lalu pastikan `config.yaml` berisi `tables_file: configs/tables.yaml`.
- Gunakan `configs/tables.example.yaml` sebagai template table list baru.

## 5. Rename Column Mapping

Jika nama kolom Oracle dan PostgreSQL berbeda tapi dianggap equivalent, isi `rename_columns`.

Format mapping adalah Oracle column ke PostgreSQL column:

```yaml
rename_columns:
  public.sample_customer:
    legacy_status: status
```

Dengan rule ini, Oracle `LEGACY_STATUS` dibandingkan dan disync ke PostgreSQL `status`.

## 6. Audit Pertama

Jalankan audit metadata dan rowcount:

```bash
python -m oracle_pg_sync audit --config config.yaml
```

Jika `tables` di config kosong, audit otomatis mengambil semua table dari PostgreSQL schema yang diset di config.

Kalau `config.yaml` tetap punya table list tapi ingin compare semua table yang ada di PostgreSQL schema:

```bash
python -m oracle_pg_sync audit --config config.yaml --all-postgres-tables --fast-count
python -m oracle_pg_sync audit --config config.yaml --all-postgres-tables --limit 10 --fast-count
```

Audit table tertentu:

```bash
python -m oracle_pg_sync audit --config config.yaml --tables sample_customer sample_order
```

Gunakan fast count untuk table besar:

```bash
python -m oracle_pg_sync audit --config config.yaml --fast-count
```

Gunakan exact count hanya jika siap dengan query berat:

```bash
python -m oracle_pg_sync audit --config config.yaml --exact-count
```

Untuk membuat SQL suggestion seperti script `verify_oracle_pg.py` lama:

```bash
python -m oracle_pg_sync audit --config config.yaml --sql-out reports/schema_suggestions.sql
python -m oracle_pg_sync audit --config config.yaml --suggest-drop
```

Untuk audit parallel, naikkan worker secara sadar karena setiap worker membuka koneksi Oracle dan PostgreSQL:

```bash
python -m oracle_pg_sync audit --config config.yaml --workers 4 --fast-count
```

Untuk compare object schema seperti view, sequence, procedure/function, package, trigger:

```bash
python -m oracle_pg_sync audit-objects --config config.yaml
python -m oracle_pg_sync audit-objects --config config.yaml --types view sequence procedure function trigger
python -m oracle_pg_sync audit-objects --config config.yaml --include-extension-objects
```

## 7. Baca Hasil Audit

File utama:

```text
reports/inventory_summary.csv
reports/report.html
reports/column_diff.csv
reports/type_mismatch.csv
reports/schema_suggestions.sql
```

Interpretasi cepat:

- `MATCH`: struktur dan rowcount match.
- `WARNING`: struktur match, tapi rowcount tidak match atau count tidak lengkap.
- `MISMATCH`: ada missing column, extra column, atau type mismatch.
- `MISSING`: table tidak ada di Oracle atau PostgreSQL.

## 8. Sync Dry-Run

Secara default sync aman karena dry-run. Command ini belum mengubah data:

```bash
python -m oracle_pg_sync sync --config config.yaml --direction oracle-to-postgres --tables sample_customer
```

Untuk table list lokal yang terpisah dari `config.yaml`:

```bash
python -m oracle_pg_sync sync --config config.yaml --direction oracle-to-postgres --tables-file configs/tables.yaml
python -m oracle_pg_sync sync --config config.yaml --direction oracle-to-postgres --tables-file configs/tables.yaml --limit 10
```

`configs/tables.yaml` boleh dibuat simple:

```yaml
tables:
  - public.address
  - public.housemaster
  - public.a_hp_house_info
```

Reverse sync PostgreSQL ke Oracle juga dry-run secara default:

```bash
python -m oracle_pg_sync sync --config config.yaml --direction postgres-to-oracle --tables sample_customer --mode truncate
```

Untuk job incremental manual dari PostgreSQL ke Oracle tanpa memasukkan filter ke `config.yaml`,
pakai runtime override pada satu table per command:

```bash
ops sync --config config.yaml --direction postgres-to-oracle --tables public.address --mode upsert --key-columns address_id --incremental-column last_update --where "last_update >= CURRENT_TIMESTAMP - INTERVAL '5 minutes'" --incremental --go
```

Lihat hasilnya di:

```text
reports/sync_result.csv
reports/sync.log
```

Status `DRY_RUN` berarti tool hanya melakukan precheck dan memberi tahu apa yang akan dilakukan.

## 9. Sync Execute

Eksekusi sungguhan wajib pakai `--execute`:

```bash
python -m oracle_pg_sync sync --config config.yaml --direction oracle-to-postgres --tables sample_customer --execute
python -m oracle_pg_sync sync --config config.yaml --direction postgres-to-oracle --tables sample_customer --mode truncate --execute
python -m oracle_pg_sync sync --config config.yaml --direction postgres-to-oracle --tables sample_customer --mode upsert --where "updated_at >= NOW() - INTERVAL '5 minutes'" --execute
```

## 10. Checkpoint, Incremental, Checksum, dan LOB

Lihat run checkpoint:

```bash
python -m oracle_pg_sync sync --config config.yaml --list-runs
```

Resume run gagal:

```bash
python -m oracle_pg_sync sync --config config.yaml --resume RUN_ID --execute
```

Incremental sync berbasis config table:

```bash
python -m oracle_pg_sync sync --config config.yaml --tables-file configs/tables.yaml --incremental
python -m oracle_pg_sync sync --config config.yaml --tables-file configs/tables.yaml --incremental --execute
```

Scheduler shortcut:

```bash
jobs/daily.sh
jobs/every_5min.sh
```

Keduanya memakai profile CLI, lock file, dan log rotation. Override config path dengan `CONFIG_PATH=/path/config.yaml`.

Cek watermark:

```bash
python -m oracle_pg_sync sync --config config.yaml --watermark-status
```

Contoh table dengan BLOB `BLOB_PAYLOAD` dibuat `NULL`, incremental `updated_at`, dan checksum tanpa BLOB:

```yaml
tables:
  - source_schema: SAMPLE_APP
    source_table: SAMPLE_BLOB_TABLE
    target_schema: public
    target_table: sample_blob_table
    primary_key:
      - record_id
    incremental:
      enabled: true
      strategy: updated_at
      column: updated_at
      overlap_minutes: 10
    lob_strategy:
      columns:
        BLOB_PAYLOAD: null
    validation:
      checksum:
        enabled: true
        mode: chunk
        exclude_columns:
          - BLOB_PAYLOAD
```

Setiap audit/sync/all membuat manifest:

```text
reports/run_<timestamp>_<run_id>/manifest.json
```

Jika struktur mismatch, table akan di-skip. Pakai `--force` hanya setelah DBA menyetujui risiko:

```bash
python -m oracle_pg_sync sync --config config.yaml --direction oracle-to-postgres --tables sample_customer --execute --force
```

## 10. Generate Report Ulang

Jika CSV sudah ada dan ingin regenerate HTML:

```bash
python -m oracle_pg_sync report --config config.yaml
```

## 11. Full Flow

Audit, sync, audit ulang, report:

```bash
python -m oracle_pg_sync all --config config.yaml --execute
```

Untuk production, lebih disarankan jalankan bertahap:

```bash
python -m oracle_pg_sync audit --config config.yaml --fast-count
python -m oracle_pg_sync sync --config config.yaml --direction oracle-to-postgres
python -m oracle_pg_sync sync --config config.yaml --direction oracle-to-postgres --execute
python -m oracle_pg_sync audit --config config.yaml --exact-count
python -m oracle_pg_sync report --config config.yaml
```

## 12. Mode Sync

`truncate`

- Truncate target lalu load ulang.
- Cepat, tapi destructive.
- Butuh `--execute`.
- Menjaga index, trigger, grants, dan view/materialized view dependency karena table object tidak diganti.
- Memakai PostgreSQL `pg_lock_timeout` agar gagal cepat kalau table sedang terkunci.
- Berlaku untuk dua arah.

`swap`

- Buat table staging `table__load`.
- Copy data ke staging.
- Verify rowcount staging.
- Rename live table menjadi old table.
- Rename staging menjadi live table.
- Tidak default karena butuh storage staging, index staging, WAL/temp, dan old table selama transaksi.
- Execute di-skip kecuali `sync.allow_swap: true` atau command memakai `--force`.
- Dry-run menampilkan estimasi storage tambahan jika ukuran table bisa dibaca dari PostgreSQL.
- Saat ini hanya aktif untuk Oracle ke PostgreSQL.

`append`

- Insert data baru ke target tanpa delete/truncate.
- Cocok untuk table log/history.
- Tidak mencegah duplicate.
- Berlaku untuk dua arah.

`upsert`

- Load staging lalu `INSERT ... ON CONFLICT`.
- Wajib `key_columns`.
- PostgreSQL harus punya unique index/constraint sesuai key.
- Untuk PostgreSQL ke Oracle, memakai Oracle `MERGE`.

`delete`

- Khusus PostgreSQL ke Oracle.
- Menjalankan `DELETE FROM target` lalu insert ulang.
- Bisa rollback dalam transaction, tapi lebih berat daripada truncate.

## 13. Output Folder

Semua output masuk ke folder dari `reports.output_dir`, default:

```text
reports/
```

File credential tidak pernah ditulis ke report.
