# Production Runbook

Gunakan runbook ini saat menjalankan audit dan sync di environment production.

Runbook ini berlaku untuk dua arah:

- `oracle-to-postgres`
- `postgres-to-oracle`

## Prinsip Safety

- Jangan jalankan sync production tanpa `audit` terlebih dahulu.
- Jangan pakai `--execute` sebelum dry-run sukses.
- Jangan pakai `--force` kecuali mismatch sudah direview.
- Jangan aktifkan `truncate_cascade` tanpa approval DBA.
- Default sync memakai `truncate`, bukan `swap`, agar tidak membuat staging table besar dan object dependency tetap menempel.
- Jalankan table besar di maintenance window.
- Pastikan backup atau restore point tersedia.

## Checklist Sebelum Run

- `.env` menunjuk ke Oracle dan PostgreSQL production yang benar.
- `config.yaml` berisi table target yang benar.
- `reports.output_dir` diarahkan ke folder run saat ini jika perlu arsip.
- User PostgreSQL punya privilege `CREATE`, `TRUNCATE`, `INSERT`, `UPDATE`, `ANALYZE`.
- Disk PostgreSQL cukup untuk staging dan old table.
- Tidak ada job lain yang menulis besar ke table target.
- Aplikasi downstream sudah tahu window sync.

## Step 1. Validasi Config

```bash
python -m oracle_pg_sync --help
python -m oracle_pg_sync audit --help
```

Cek table list di `config.yaml`.

Table list bawaan sudah dimigrasikan dari:

- `example/ora2pg.py` untuk `oracle-to-postgres`.
- `example/pg2ora.py` untuk `postgres-to-oracle`.

Jika menjalankan sync tanpa `--tables`, CLI akan memilih table berdasarkan field `directions`.

## Step 2. Audit Awal

Untuk banyak table atau table besar:

```bash
python -m oracle_pg_sync audit --config config.yaml --fast-count
```

Untuk subset:

```bash
python -m oracle_pg_sync audit --config config.yaml --tables ADDRESS HOUSEMASTER --fast-count
```

Buka:

```text
reports/report.html
reports/inventory_summary.csv
reports/column_diff.csv
reports/type_mismatch.csv
```

Stop jika ada:

- `MISSING`
- `MISMATCH`
- missing columns
- extra columns yang belum disetujui
- type mismatch fatal

## Step 3. Dry-Run Sync

```bash
python -m oracle_pg_sync sync --config config.yaml --direction oracle-to-postgres
```

Atau table tertentu:

```bash
python -m oracle_pg_sync sync --config config.yaml --direction oracle-to-postgres --tables ADDRESS
```

Untuk reverse sync PostgreSQL ke Oracle:

```bash
python -m oracle_pg_sync sync --config config.yaml --direction postgres-to-oracle --tables ADDRESS --mode truncate
```

Pastikan `reports/sync_result.csv` berisi `DRY_RUN`, bukan `FAILED`.

## Step 4. Execute Sync

```bash
python -m oracle_pg_sync sync --config config.yaml --direction oracle-to-postgres --execute
```

Untuk satu table:

```bash
python -m oracle_pg_sync sync --config config.yaml --direction oracle-to-postgres --tables ADDRESS --execute
```

Execute reverse sync:

```bash
python -m oracle_pg_sync sync --config config.yaml --direction postgres-to-oracle --tables ADDRESS --mode truncate --execute
```

Pantau:

```bash
tail -f reports/sync.log
```

## Step 5. Audit Ulang

Setelah sync:

```bash
python -m oracle_pg_sync audit --config config.yaml --exact-count
python -m oracle_pg_sync report --config config.yaml
```

Cek `report.html`.

Kriteria sukses:

- Table penting status `MATCH`.
- `sync_result.csv` tidak memiliki `FAILED`.
- Rowcount critical table match.
- Tidak ada type mismatch baru.

## Step 6. Arsip Report

Contoh:

```bash
mkdir -p run-logs/$(date +%Y%m%d_%H%M%S)
cp reports/* run-logs/$(date +%Y%m%d_%H%M%S)/ 2>/dev/null || true
```

## Lock dan Resource Policy

- Default `parallel_workers: 1`, jadi tidak membuka banyak koneksi/load bersamaan.
- Default `fast_count: true` untuk audit ringan.
- Default `exact_count_after_load: false`; exact count dijalankan manual saat window validasi.
- Default `pg_lock_timeout: 5s`; jika PostgreSQL tidak bisa ambil lock untuk truncate, sync gagal cepat.
- Mode `truncate` tidak membuat staging table, jadi menghindari kasus storage penuh karena `__load`/`__old`.

## Rollback Mode Swap

Jika `keep_old_after_swap: true`, old table disimpan dengan suffix:

```text
table__old_YYYYMMDDHHMMSS
```

Rollback manual PostgreSQL:

```sql
BEGIN;
LOCK TABLE public.sample_customer IN ACCESS EXCLUSIVE MODE;
ALTER TABLE public.sample_customer RENAME TO sample_customer__bad_20260101010101;
ALTER TABLE public.sample_customer__old_20260101000000 RENAME TO sample_customer;
COMMIT;
```

Sesuaikan nama table old dari database actual.

## Rollback Mode Truncate

Mode `truncate` tidak menyimpan old table. Rollback membutuhkan backup/restore eksternal.

Karena itu untuk production full refresh, prioritaskan `swap`.

## Rollback Mode Append

Rollback append tergantung ada tidaknya batch marker atau timestamp. Jika tidak ada marker, rollback sulit.

Disarankan tambahkan filter `where` dan audit hasil sebelum append production.

## Rollback Mode Upsert

Upsert mengubah row existing. Rollback membutuhkan backup atau audit trail.

## Kapan Pakai --force

Pakai `--force` hanya jika semua kondisi ini terpenuhi:

- Column mismatch sudah dipahami.
- Kolom yang tidak match tidak dibutuhkan untuk load.
- Type mismatch tidak menyebabkan data loss.
- DBA menyetujui.
- Ada backup atau rollback plan.

Command:

```bash
python -m oracle_pg_sync sync --config config.yaml --direction oracle-to-postgres --tables ADDRESS --mode swap --execute --force
```

## Catatan Reverse Sync PostgreSQL ke Oracle

- Mode yang didukung: `truncate`, `append`, `delete`, `upsert`.
- Mode `swap` tidak diaktifkan untuk Oracle karena berisiko merusak grants, views, triggers, synonyms, dan dependency.
- `truncate` di Oracle melakukan implicit commit; gunakan hanya saat rollback eksternal tersedia.
- `delete` lebih berat, tapi lebih mudah dikontrol dalam transaction.
- `upsert` memakai Oracle `MERGE` dan wajib `key_columns`.

## Rekomendasi Table Besar

- Audit awal pakai `--fast-count`.
- Jalankan sync per table, bukan semua sekaligus.
- Set `parallel_workers: 1`.
- Pastikan disk cukup untuk staging plus old table.
- Jalankan exact count setelah load hanya saat window cukup.
- Monitor lock di PostgreSQL.

Query monitor lock:

```sql
SELECT pid, wait_event_type, wait_event, query
FROM pg_stat_activity
WHERE datname = current_database()
ORDER BY query_start NULLS LAST;
```
