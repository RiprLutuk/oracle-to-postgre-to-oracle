# Report Reference

Semua output eksekusi masuk ke folder `reports/run_<timestamp>_<run_id>/`
kecuali `reports.output_dir` diubah. Root `reports/` hanya dipakai untuk
checkpoint, lock, log runtime global, dan kumpulan run folder.

## Daftar File

`run_<timestamp>_<run_id>/inventory_summary.csv`

- Summary utama per table.
- Dipakai untuk melihat status final table.

`run_<timestamp>_<run_id>/inventory_summary.xlsx`

- Versi Excel dari inventory summary.
- Cocok untuk review DBA/non-developer.

`run_<timestamp>_<run_id>/column_diff.csv`

- Daftar missing column, extra column, dan ordinal mismatch.

`run_<timestamp>_<run_id>/type_mismatch.csv`

- Daftar tipe data yang dianggap tidak compatible.

`run_<timestamp>_<run_id>/object_dependency_summary.csv`

- Object terkait table dari Oracle dan PostgreSQL.
- Berisi view, procedure, function, package, atau dependency lain yang terdeteksi.

`run_<timestamp>_<run_id>/object_inventory.csv`

- Inventory object schema dari `audit-objects`.
- Berisi view, materialized view, sequence, procedure, function, package, trigger, synonym.

`run_<timestamp>_<run_id>/object_compare.csv`

- Hasil compare object schema Oracle vs PostgreSQL dari `audit-objects`.
- Status: `MATCH`, `MISSING_IN_ORACLE`, atau `MISSING_IN_POSTGRES`.
- PostgreSQL extension-owned objects di-skip secara default; pakai `--include-extension-objects` jika perlu.

`run_<timestamp>_<run_id>/sync_result.csv`

- Hasil command sync.
- Ada status per table: `DRY_RUN`, `SUCCESS`, `WARNING`, `SKIPPED`, `FAILED`.

`run_<timestamp>_<run_id>/logs.txt`

- Log runtime.
- Dipakai untuk investigasi error.

`run_<timestamp>_<run_id>/validation_checksum.csv`

- Hasil checksum validation jika fitur checksum aktif.
- Field penting: `table_name`, `chunk_key`, `source_hash`, `target_hash`, `row_count_source`, `row_count_target`, `status`.

`run_<timestamp>_<run_id>/manifest.json`

- Manifest durable setiap audit/sync/all.
- Berisi run id, command, durasi, git commit, config hash, scope table,
  rows summary, checksum/lob summary, checkpoint path, report files, dan errors.
- Password/secret tidak ditulis.

`run_<timestamp>_<run_id>/report.xlsx`

- Centralized Excel workbook untuk audit/sync run.
- Header frozen, auto-filter aktif, width kolom otomatis, dan status penting diberi warna.
- Tidak menulis password/secret atau raw LOB content.

Sheet:

- `00_Dashboard`: ringkasan total table, success/failed, checksum, row processed, watermark, dan resume usage.
- `01_Run_Summary`: summary run dengan duration, warning, dan dry-run count.
- `02_Table_Sync_Status`: status per table dari sync atau inventory audit.
- `03_Rowcount_Compare`: rowcount Oracle/PostgreSQL dan status match.
- `04_Checksum_Result`: hasil checksum table/chunk.
- `05_Column_Diff`: missing/extra/ordinal/type mismatch.
- `06_Index_Compare`: dependency/index rows yang terdeteksi.
- `07_Object_Dependency`: view, materialized view, procedure, function, package, dan sequence dependency.
- `08_LOB_Columns`: kolom LOB dari sync atau `ops analyze lob`, strategy,
  target type, validation mode, classification, warning, dan recommendation.
- `09_Failed_Tables`: table dengan status `FAILED`, `MISMATCH`, atau `MISSING`.
- `10_Watermark`: watermark tersimpan saat run ditulis.
- `11_Checkpoint`: chunk/checkpoint status.
- `12_Performance`: elapsed time, rows loaded, dan rows/second.
- `13_Errors`: error sync dan dependency maintenance.
- `14_Config`: config sanitized untuk audit trail.

`run_<timestamp>_<run_id>/report.html`

- Dashboard HTML per run.
- Berisi link lokal ke `report.xlsx` dan `manifest.json`.

## inventory_summary.csv

Field:

`table_name`

- Nama table PostgreSQL dalam format `schema.table`.

`oracle_exists`

- `true` jika table Oracle ada.

`postgres_exists`

- `true` jika table PostgreSQL ada.

`oracle_row_count`

- Rowcount Oracle.
- Bisa exact atau fast count tergantung config/flag.

`postgres_row_count`

- Rowcount PostgreSQL.
- Bisa exact atau fast count tergantung config/flag.

`row_count_match`

- `true` jika rowcount Oracle dan PostgreSQL sama.

`oracle_column_count`

- Jumlah kolom Oracle.

`postgres_column_count`

- Jumlah kolom PostgreSQL.

`column_structure_match`

- `true` jika tidak ada missing/extra column dan ordinal sama.

`type_mismatch_count`

- Jumlah kolom yang tipe datanya tidak compatible.

`missing_columns_in_pg`

- Kolom Oracle yang tidak ditemukan di PostgreSQL.

`extra_columns_in_pg`

- Kolom PostgreSQL yang tidak ditemukan di Oracle.

`index_count_oracle`

- Jumlah index Oracle pada table.

`index_count_postgres`

- Jumlah index PostgreSQL pada table.

`view_count_related_oracle`

- Jumlah Oracle view terkait.

`view_count_related_postgres`

- Jumlah PostgreSQL view/materialized view terkait.

`sequence_count_oracle`

- Jumlah sequence Oracle yang namanya terkait table.

`sequence_count_postgres`

- Jumlah sequence PostgreSQL yang namanya terkait table.

`stored_procedure_count_related_oracle`

- Jumlah procedure/package Oracle terkait.

`function_count_related_postgres`

- Jumlah function PostgreSQL terkait.

`trigger_count_oracle`

- Jumlah trigger Oracle pada table.

`trigger_count_postgres`

- Jumlah trigger PostgreSQL pada table.

`constraint_count_oracle`

- Jumlah constraint Oracle pada table.

`constraint_count_postgres`

- Jumlah constraint PostgreSQL pada table.

`status`

- Status final audit.

## Status Audit

`MATCH`

- Table ada di Oracle dan PostgreSQL.
- Struktur kolom match.
- Tipe compatible.
- Rowcount match.

`WARNING`

- Struktur match, tapi rowcount tidak match.
- Bisa juga terjadi jika count tidak tersedia lengkap.
- Perlu review sebelum dianggap sukses.

`MISMATCH`

- Ada missing column, extra column, atau type mismatch.
- Sync default akan skip table ini kecuali `--force`.

`MISSING`

- Table tidak ada di salah satu database.

## column_diff.csv

Field:

`table_name`

- Table yang dibandingkan.

`diff_type`

- `missing_in_postgres`: kolom ada di Oracle tapi tidak ada di PostgreSQL.
- `extra_in_postgres`: kolom ada di PostgreSQL tapi tidak ada di Oracle.
- `ordinal_mismatch`: posisi kolom berbeda.

`column_name`

- Nama kolom normalized.

`oracle_type`

- Tipe Oracle.

`postgres_type`

- Tipe PostgreSQL.

`suggested_pg_type`

- Saran tipe PostgreSQL jika kolom perlu dibuat.

## schema_suggestions.sql

File ini dibuat otomatis dari `column_diff.csv`.

- `missing_in_postgres` menjadi saran `ALTER TABLE ... ADD COLUMN`.
- `extra_in_postgres` hanya menjadi `ALTER TABLE ... DROP COLUMN` jika audit dijalankan dengan `--suggest-drop`.
- Review manual tetap wajib sebelum SQL dijalankan di production.

## type_mismatch.csv

Field:

`table_name`

- Table yang dibandingkan.

`column_name`

- Kolom yang mismatch.

`oracle_type`

- Tipe Oracle.

`postgres_type`

- Tipe PostgreSQL.

`reason`

- Alasan mismatch.

## sync_result.csv

Field:

`table_name`

- Table yang disync.

`mode`

- Mode sync yang dipakai.

`direction`

- Arah sync: `oracle-to-postgres` atau `postgres-to-oracle`.

`status`

- `DRY_RUN`, `SUCCESS`, `WARNING`, `SKIPPED`, atau `FAILED`.

`rows_loaded`

- Jumlah row yang dikirim ke PostgreSQL.

`oracle_row_count`

- Exact count Oracle setelah load jika verification aktif.

`postgres_row_count`

- Exact count PostgreSQL setelah load jika verification aktif.

`row_count_match`

- Hasil verifikasi exact count.

`dry_run`

- `true` jika tidak ada perubahan data.

`message`

- Pesan tambahan atau error.

`elapsed_seconds`

- Durasi proses table.

Field tambahan production safety:

- `run_id`
- `checksum_status`
- `lob_columns_detected`
- `lob_strategy_applied`
- `lob_columns_skipped`
- `lob_columns_nullified`

## Cara Review Report

Urutan review yang disarankan:

1. Buka `report.html`.
2. Cek jumlah `MISMATCH` dan `MISSING`.
3. Buka `column_diff.csv` untuk mismatch struktur.
4. Buka `type_mismatch.csv` untuk mismatch tipe.
5. Buka `sync_result.csv` untuk table gagal sync.
6. Untuk table critical, validasi rowcount exact.

## Fuzzy Type Compatibility

Contoh yang dianggap compatible:

- Oracle `VARCHAR2(50)` ke PostgreSQL `varchar(100)` atau `text`.
- Oracle `NUMBER(9,0)` ke PostgreSQL `integer`.
- Oracle `NUMBER(18,0)` ke PostgreSQL `bigint`.
- Oracle `DATE` ke PostgreSQL `timestamp`.
- Oracle `CLOB` ke PostgreSQL `text`.
- Oracle `BLOB` atau `RAW` ke PostgreSQL `bytea`.

Contoh yang dianggap mismatch:

- Oracle `VARCHAR2(100)` ke PostgreSQL `varchar(50)`.
- Oracle `NUMBER(18,0)` ke PostgreSQL `integer`.
- Oracle `NUMBER(10,2)` ke PostgreSQL `integer`.
