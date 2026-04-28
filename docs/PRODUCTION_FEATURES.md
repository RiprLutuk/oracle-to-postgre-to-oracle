# Production Safety Features

Fitur di halaman ini membuat sync lebih aman untuk run production besar. Default tetap aman: `sync` adalah dry-run kecuali command diberi `--execute`.

## Checkpoint dan Resume

Setiap sync execute membuat `run_id` dan menyimpan status run/chunk di SQLite:

```text
reports/checkpoints/checkpoint.sqlite3
```

Lihat run:

```bash
python -m oracle_pg_sync sync --config config.yaml --list-runs
```

Resume run gagal:

```bash
python -m oracle_pg_sync sync --config config.yaml --resume RUN_ID --execute
```

Reset checkpoint:

```bash
python -m oracle_pg_sync sync --config config.yaml --reset-checkpoint RUN_ID
```

Untuk table yang punya `key_columns`, Oracle ke PostgreSQL dapat diproses per range key sehingga chunk yang sudah `success` tidak diulang saat resume. Jika tidak ada key, tool menyimpan satu chunk `full`.

## Incremental Sync

Config table:

```yaml
tables:
  - name: public.sample_customer
    key_columns: [customer_id]
    incremental:
      enabled: true
      strategy: updated_at
      column: updated_at
      initial_value: null
      overlap_minutes: 10
      delete_detection: false
```

Run incremental:

```bash
python -m oracle_pg_sync sync --config config.yaml --incremental --execute
```

Full refresh tanpa filter watermark:

```bash
python -m oracle_pg_sync sync --config config.yaml --full-refresh --execute
```

Cek/reset watermark:

```bash
python -m oracle_pg_sync sync --config config.yaml --watermark-status
python -m oracle_pg_sync sync --config config.yaml --reset-watermark public.sample_customer
```

Strategi:

- `updated_at`: memakai nilai max timestamp setelah sync sukses. Overlap window diterapkan saat membaca watermark agar update terlambat tidak terlewat.
- `numeric_key`: memakai nilai max numeric key/id. Cocok untuk append-only table.
- `oracle_scn`: config/interface tersedia, tetapi tool akan gagal jelas karena implementasi Flashback/SCN belum aktif.

Watermark hanya diupdate setelah sync sukses dan validasi tidak gagal.

## Checksum Validation

Config global atau table-level:

```yaml
validation:
  checksum:
    enabled: true
    mode: table
    columns: auto
    exclude_columns:
      - BLOB_PAYLOAD
    sample_percent: 1
```

Checksum memakai hash stabil dari kolom comparable. Tipe LOB dan JSON yang tidak stabil otomatis dikeluarkan pada mode `columns: auto`.

Jika checksum mismatch setelah load, table dianggap gagal dan watermark tidak diupdate.

## LOB Strategy

Default sync untuk LOB adalah `error`: fail early sebelum data diubah. Ini disengaja agar BLOB/CLOB besar tidak tersalin tanpa keputusan DBA.

Pilihan:

- `skip`: kolom LOB tidak ikut select/insert.
- `null`: kolom target tetap diisi, tetapi value dibuat `NULL`.
- `stream`: copy LOB seperti biasa dengan pembacaan driver.
- `error`: fail early.

Contoh DBA use case: Oracle `SAMPLE_BLOB_TABLE.BLOB_PAYLOAD` disync sebagai `NULL` dan dikeluarkan dari checksum.

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
        mode: batch
        exclude_columns:
          - BLOB_PAYLOAD
```

Report `sync_result.csv` berisi kolom:

- `lob_columns_detected`
- `lob_strategy_applied`
- `lob_columns_skipped`
- `lob_columns_nullified`

## Run Manifest

Setiap audit/sync/all membuat manifest:

```text
reports/run_<timestamp>_<run_id>/manifest.json
```

Manifest berisi command, waktu mulai/selesai, durasi, git commit, hash config, scope table, ringkasan rows, checkpoint path, report files, dan error. Password/secret disanitasi sebelum ditulis.

`report.html` otomatis menautkan manifest terbaru jika ada.

## Safe Production Run Example

```bash
python -m oracle_pg_sync audit --config config.yaml --tables-file configs/tables.yaml --fast-count
python -m oracle_pg_sync sync --config config.yaml --tables-file configs/tables.yaml --incremental
python -m oracle_pg_sync sync --config config.yaml --tables-file configs/tables.yaml --incremental --execute
python -m oracle_pg_sync sync --config config.yaml --resume RUN_ID --execute
python -m oracle_pg_sync audit --config config.yaml --tables-file configs/tables.yaml --exact-count
```

## CI

GitHub Actions menjalankan unit test tanpa koneksi Oracle/PostgreSQL real. Database behavior baru ditest dengan unit test, stub, dan SQLite lokal untuk checkpoint.
