# DBA Daily Operations Guide

Panduan ini dibuat untuk operasi harian DBA/ops. Fokusnya bukan setup awal,
melainkan keputusan dan command saat menjalankan audit, sync, validasi,
rollback, dan recovery.

Gunakan `ops` sebagai command utama. Semua contoh mengasumsikan project sudah
di-install editable atau shell sudah berada di root project.

## Prinsip Utama

- Satu run = satu folder `reports/run_<timestamp>_<run_id>/`.
- Untuk investigasi, selalu pakai file di folder run tersebut.
- Jangan menyimpulkan satu run dari `reports/sync.log`; itu log global lintas run.
- `ops sync` tanpa `--go` adalah dry-run.
- Execute sungguhan hanya terjadi dengan `--go`.
- `SUCCESS` hanya boleh dianggap bersih jika copy selesai, rowcount valid, tidak ada failed rows, dan checksum tidak mismatch.
- `WARNING` berarti ada hal yang harus dibaca; bukan otomatis gagal, tapi juga bukan lampu hijau mutlak.
- `FAILED`, `MISMATCH`, atau `data_integrity_status=FAIL` harus ditangani sebagai issue data.

## Command Map Harian

| Tujuan | Command |
| --- | --- |
| Cek config, env, koneksi, privilege | `ops doctor --config config.yaml` |
| Audit schema, rowcount, dependency | `ops audit --config config.yaml --exact-count` |
| Audit satu table | `ops audit --config config.yaml --tables public.table_name --exact-count` |
| Analisa LOB | `ops analyze lob --config config.yaml --tables public.table_name` |
| Dry-run sync | `ops sync --config config.yaml --tables public.table_name --mode truncate_safe` |
| Simulasi risiko | `ops sync --config config.yaml --tables public.table_name --mode truncate_safe --simulate` |
| Execute sync | `ops sync --config config.yaml --tables public.table_name --mode truncate_safe --go` |
| Validasi rowcount setelah sync | `ops validate --config config.yaml --tables public.table_name` |
| Compare missing/extra key | `ops validate missing-keys --config config.yaml --tables public.table_name` |
| Lihat run terakhir | `ops status --config config.yaml` |
| Buka path report terakhir | `ops report latest --config config.yaml` |
| Lihat watermark incremental | `ops watermarks --config config.yaml` |
| Reset watermark table | `ops reset-watermark public.table_name --config config.yaml` |
| Lihat circuit breaker | `ops circuit-breaker list --config config.yaml` |
| Reset circuit breaker per table | `ops circuit-breaker reset --table A_HP_BATCH --config config.yaml` |
| Reset semua circuit breaker | `ops circuit-breaker reset --all --config config.yaml` |
| Rollback run safe mode | `ops rollback RUN_ID --config config.yaml` |
| Repair dependency | `ops dependencies repair --config config.yaml` |

## Workflow Standar Sebelum Execute

Gunakan urutan ini untuk table production yang belum pernah disync atau baru
berubah mapping/config-nya:

```bash
ops doctor --config config.yaml
ops audit --config config.yaml --tables public.table_name --exact-count
ops analyze lob --config config.yaml --tables public.table_name
ops sync --config config.yaml --direction oracle-to-postgres --tables public.table_name --mode truncate_safe --simulate
ops sync --config config.yaml --direction oracle-to-postgres --tables public.table_name --mode truncate_safe
```

Jika semua jelas, baru execute:

```bash
ops sync --config config.yaml --direction oracle-to-postgres --tables public.table_name --mode truncate_safe --go
```

Setelah execute:

```bash
ops sync --config config.yaml --direction oracle-to-postgres --tables public.table_name --rowcount-only
ops validate missing-keys --config config.yaml --direction oracle-to-postgres --tables public.table_name
ops report latest --config config.yaml
```

Untuk LOB yang memang harus dipakai aplikasi:

```bash
ops sync \
  --config config.yaml \
  --direction oracle-to-postgres \
  --tables public.table_name \
  --mode truncate_safe \
  --lob stream \
  --go
```

## PostgreSQL -> Oracle Operations

Reverse sync dipakai saat PostgreSQL menjadi source dan Oracle menjadi target.
Jalur ini sudah mendukung load data, Oracle `MERGE` untuk upsert, checkpoint
fase table, checksum opsional, rowcount validation, missing-key compare, dan
profile job direction-aware.

Mode reverse yang didukung:

| Mode | Status | Catatan |
| --- | --- | --- |
| `upsert` | direkomendasikan untuk incremental/reverse rutin | memakai Oracle `MERGE`, wajib `key_columns` |
| `append` | untuk insert-only | tidak mencegah duplikasi |
| `truncate` | full replace Oracle | destructive langsung, pakai hanya saat window aman |
| `delete` | full delete lalu insert dalam transaksi | lebih transactional daripada truncate, tapi tetap menghapus target |
| `swap` | tidak direkomendasikan | diskip karena berisiko untuk grants/views/triggers Oracle |
| `truncate_safe`, `swap_safe`, `incremental_safe` | bukan mode reverse utama | safe-mode backup/cutover saat ini difokuskan ke Oracle -> PostgreSQL |

### Reverse Upsert Harian

Contoh manual dry-run:

```bash
ops sync \
  --config config.yaml \
  --direction postgres-to-oracle \
  --tables public.sample_customer \
  --mode upsert \
  --key-columns customer_id \
  --incremental-column updated_at \
  --incremental
```

Execute:

```bash
ops sync \
  --config config.yaml \
  --direction postgres-to-oracle \
  --tables public.sample_customer \
  --mode upsert \
  --key-columns customer_id \
  --incremental-column updated_at \
  --incremental \
  --go
```

Post-sync verification:

```bash
ops validate --config config.yaml --direction postgres-to-oracle --tables public.sample_customer
ops validate missing-keys --config config.yaml --direction postgres-to-oracle --tables public.sample_customer
ops watermarks --config config.yaml
```

### Reverse Full Replace

Pakai hanya jika app owner menyetujui Oracle target boleh diganti penuh.

Dry-run:

```bash
ops sync --config config.yaml --direction postgres-to-oracle --tables public.table_name --mode truncate
```

Execute:

```bash
ops sync --config config.yaml --direction postgres-to-oracle --tables public.table_name --mode truncate --go
```

Alternatif transactional delete + insert:

```bash
ops sync --config config.yaml --direction postgres-to-oracle --tables public.table_name --mode delete --go
```

### Reverse Job Wrapper

Incremental reverse:

```bash
CONFIG_PATH=/path/to/config.yaml jobs/incremental.sh pg_to_oracle \
  --tables public.sample_customer \
  --mode upsert \
  --key-columns customer_id \
  --incremental-column updated_at \
  --incremental
```

Jika `--mode` tidak diisi, profile `every_5min` dengan direction
`postgres-to-oracle` sekarang memilih `upsert`. Tetap lebih baik menulis
`--mode upsert` eksplisit di cron agar niat operasional terlihat.

Full daily reverse:

```bash
CONFIG_PATH=/path/to/config.yaml jobs/daily.sh pg_to_oracle --tables public.table_name --mode truncate
```

### Reverse Guardrails

- Jangan pakai `upsert` tanpa `key_columns`.
- Pastikan Oracle punya privilege insert/update/delete/truncate sesuai mode.
- Pastikan key yang dipakai `MERGE` benar-benar unik secara bisnis.
- Delete di PostgreSQL source tidak otomatis menghapus Oracle target saat mode `upsert`.
- Untuk delete propagation, perlu workflow terpisah atau full replace terjadwal.
- LOB reverse mengikuti metadata target Oracle; test dulu dengan `ops analyze lob`.
- Rollback otomatis reverse belum setara safe mode Oracle -> PostgreSQL. Untuk reverse production, siapkan backup/restore DBA native jika memakai `truncate` atau `delete`.
- Standalone `ops validate --direction postgres-to-oracle` membandingkan count PostgreSQL source terhadap Oracle target. Untuk incremental window, mismatch bisa normal jika target berisi data historis penuh; gunakan missing-key compare dan checksum sesuai scope.

## Validate

### Rowcount Validation

Command:

```bash
ops validate --config config.yaml --direction oracle-to-postgres --tables public.table_name
ops validate --config config.yaml --direction postgres-to-oracle --tables public.table_name
```

Output:

```text
reports/run_<timestamp>_<run_id>/rowcount_validation.csv
reports/run_<timestamp>_<run_id>/manifest.json
reports/run_<timestamp>_<run_id>/logs.txt
```

Kolom penting:

| Kolom | Arti |
| --- | --- |
| `table_name` | Nama table target yang divalidasi |
| `source_schema`, `source_table` | Object sumber yang dihitung |
| `target_schema`, `target_table` | Object target yang dihitung |
| `oracle_row_count` | Count sisi Oracle untuk arah Oracle -> PostgreSQL |
| `postgres_row_count` | Count sisi PostgreSQL |
| `row_count_match` | `true` jika count sama |
| `row_count_diff` | Selisih target - source |
| `status` | `MATCH` atau `MISMATCH` |

Exit code:

- `0`: semua rowcount match.
- `1`: minimal satu table mismatch.

Gunakan ini untuk:

- post-sync verification
- cek ulang table yang sebelumnya `FAILED`
- cek full count di maintenance window
- memastikan run incremental tidak mengurangi data tanpa alasan

Untuk arah PostgreSQL -> Oracle, `row_count_diff` dihitung sebagai
`oracle_row_count - postgres_row_count`, karena Oracle adalah target.

### Rowcount-Only Via Sync Command

Jika operator terbiasa memakai command `sync`, ada shortcut:

```bash
ops sync --config config.yaml --tables public.table_name --rowcount-only
```

Command ini tidak load data. Ia hanya membuat `rowcount_validation.csv`.

### Missing Key Compare

Command:

```bash
ops validate missing-keys --config config.yaml --direction oracle-to-postgres --tables public.table_name
ops validate missing-keys --config config.yaml --direction postgres-to-oracle --tables public.table_name
```

Alias:

```bash
ops validate --config config.yaml --tables public.table_name --missing-keys
```

Syarat:

- Table sebaiknya punya `key_columns` di config; jika tidak, CLI akan mencoba `PRIMARY KEY` lalu `UNIQUE` constraint dari Oracle/PostgreSQL.
- Key harus stabil dan unik secara bisnis.
- Untuk table besar, jalankan saat koneksi stabil karena compare melakukan full sorted streaming compare.

Output:

```text
missing_keys_summary.csv
keys_in_oracle_not_in_postgres.csv
keys_in_postgres_not_in_oracle.csv
logs.txt
```

Cara baca:

| File | Arti |
| --- | --- |
| `missing_keys_summary.csv` | Ringkasan match/mismatch per table |
| `keys_in_oracle_not_in_postgres.csv` | Key ada di source Oracle tapi tidak ada di target PostgreSQL |
| `keys_in_postgres_not_in_oracle.csv` | Key ekstra di PostgreSQL |

Status:

- `MATCH`: tidak ada key hilang/ekstra.
- `MISMATCH`: ada minimal satu key beda.
- `FAILED`: key config tidak lengkap, query gagal, atau koneksi gagal.

Catatan penting: sample CSV bisa dibatasi oleh `validation.missing_keys.sample_limit`, tetapi status summary tetap berasal dari full sorted streaming compare.

### Checksum Validation

Checksum tidak punya standalone `ops validate checksum` command saat ini. Checksum berjalan sebagai bagian dari `sync` jika config mengaktifkan:

```yaml
validation:
  checksum:
    enabled: true
    mode: table
    columns: auto
    exclude_lob_by_default: true
```

Output:

```text
validation_checksum.csv
report.xlsx
report.html
manifest.json
```

Default checksum mengecualikan LOB (`BLOB`, `CLOB`, `NCLOB`, `LONG`, `LONG RAW`, `bytea`) agar query hash tidak berat dan tidak salah format. LOB divalidasi lewat strategi LOB, biasanya `size` atau `size_hash`.

Treat checksum mismatch sebagai hard failure:

1. Cek `effective_where`.
2. Cek kolom yang di-hash.
3. Cek timezone/date precision.
4. Cek rename column.
5. Cek LOB exclusion.
6. Jalankan missing-key compare jika key tersedia.

## Post-Sync Verification

Setelah execute, buka folder run terbaru:

```bash
ops report latest --config config.yaml
```

Cek file:

```text
sync_result.csv
report.html
report.xlsx
manifest.json
logs.txt
dependency_summary.csv
dependency_post.csv
validation_checksum.csv
rollback_result.csv
metrics.json
```

### Minimum PASS Criteria

Untuk setiap table, target ideal:

| Field | Harus |
| --- | --- |
| `status` | `SUCCESS` |
| `rows_failed` | `0` |
| `rows_read_from_oracle` | sama dengan `rows_written_to_postgres` untuk Oracle -> PostgreSQL |
| `rows_written_to_postgres` | sama dengan `postgres_row_count` setelah load |
| `row_count_match` | `true` |
| `row_count_diff` | `0` |
| `validation_status` | `validation_pass` atau ekuivalen sukses |
| `data_integrity_status` | `PASS` |
| `checksum_status` | kosong atau `MATCH` |

Jika salah satu tidak terpenuhi:

- Jangan update status manual menjadi sukses.
- Jangan reset circuit breaker sebelum root cause jelas.
- Jangan reset watermark kecuali memang salah watermark dan sudah disetujui.

### Post-Sync Command Checklist

Untuk Oracle -> PostgreSQL full/safe refresh:

```bash
ops validate --config config.yaml --direction oracle-to-postgres --tables public.table_name
ops validate missing-keys --config config.yaml --direction oracle-to-postgres --tables public.table_name
ops dependencies check --config config.yaml --tables public.table_name
```

Untuk PostgreSQL -> Oracle:

```bash
ops validate --config config.yaml --direction postgres-to-oracle --tables public.table_name
ops validate missing-keys --config config.yaml --direction postgres-to-oracle --tables public.table_name
```

Untuk incremental:

```bash
ops watermarks --config config.yaml
ops validate --config config.yaml --tables public.table_name
```

Jangan hanya mengandalkan log `table_committed`. Commit berarti transaksi selesai, bukan berarti data sudah terbukti match.

## Circuit Breaker

Circuit breaker mencegah job execute yang sama terus mengubah data saat gagal
berulang. Ini aktif untuk command `sync` dan `all` saat execute.

Config:

```yaml
sync:
  max_failures: 3
  cooldown_minutes: 30
job:
  name: oracle_to_pg_daily
```

Job key dibentuk dari:

```text
job.name atau nama config file
command
direction
sorted table list
```

Contoh bentuk key:

```text
oracle_to_pg_daily:sync:oracle-to-postgres:public.sample_customer,public.sample_order
```

### Cek Circuit Breaker

```bash
ops circuit-breaker list --config config.yaml
```

Output:

```text
job_key,failure_count,last_failure_at,cooldown_until,blocked,last_error
oracle_to_pg_daily:sync:oracle-to-postgres:public.sample_customer,3,2026-04-30T10:10:00+00:00,2026-04-30T10:40:00+00:00,yes,rowcount mismatch
```

Exit code:

- `0`: tidak ada circuit yang sedang blocked.
- `1`: ada circuit yang sedang blocked.

### Reset Circuit Breaker

Reset satu job:

```bash
ops circuit-breaker reset --table public.sample_customer --config config.yaml
```

Reset semua:

```bash
ops circuit-breaker reset --all --config config.yaml
```

Reset hanya boleh dilakukan setelah:

- root cause sudah jelas
- data target sudah diverifikasi atau di-rollback
- command dry-run/simulate sudah bersih
- DBA setuju job boleh jalan lagi

### Saat Circuit Aktif

Jika job gagal karena circuit breaker:

1. Jangan langsung reset.
2. Buka run terakhir yang gagal.
3. Cek `sync_result.csv`, `logs.txt`, dan `dependency_summary.csv`.
4. Jalankan validate manual.
5. Rollback jika safe mode sudah cutover dan data target tidak valid.
6. Setelah root cause selesai, reset circuit.
7. Jalankan dry-run.
8. Baru jalankan `--go`.

## Rollback

Rollback hanya tersedia jika run mencatat rollback action di checkpoint. Safe
mode Oracle -> PostgreSQL mencatat rollback state otomatis.

Mode yang mendukung rollback:

| Mode | Rollback behavior |
| --- | --- |
| `truncate_safe` | Restore target dari backup table |
| `swap_safe` | Rename preserved backup kembali ke live table |
| `incremental_safe` | Restore pre-apply target backup |

Mode yang umumnya tidak punya rollback otomatis:

| Mode | Alasan |
| --- | --- |
| `truncate` | Target ditruncate langsung |
| `append` | Data sudah ditambahkan ke target |
| `upsert` | Row target sudah diubah berdasarkan key |
| `delete` | Khusus reverse, delete target dalam transaksi run |

### Cek Apakah Rollback Ada

1. Buka `manifest.json`.
2. Cari `rollback_summary`.
3. Buka `report.xlsx` sheet `14_Rollback` jika ada.
4. Cek `rollback_result.csv` jika rollback sudah pernah dicoba.

Atau jalankan:

```bash
ops rollback RUN_ID --config config.yaml
```

Jika tidak ada action, output akan berisi:

```text
RUN_ID,FAILED,no rollback actions found
```

### SOP Rollback

1. Stop cron/job untuk direction terkait.
2. Catat run ID gagal.
3. Copy path run folder ke tiket/incident.
4. Jalankan:

```bash
ops rollback RUN_ID --config config.yaml
```

5. Validasi rowcount:

```bash
ops validate --config config.yaml --tables public.table_name
```

6. Cek dependency:

```bash
ops dependencies check --config config.yaml --tables public.table_name
```

7. Jika dependency rusak:

```bash
ops dependencies repair --config config.yaml --tables public.table_name
```

8. Buka report rollback dan pastikan semua row `SUCCESS`.
9. Baru re-enable cron setelah dry-run berikutnya bersih.

### Hal Yang Membatalkan Keamanan Rollback

- `backup_before_truncate: false`
- backup table dihapus manual
- retention cleanup sudah menghapus backup yang dibutuhkan
- user PostgreSQL tidak punya privilege rename/truncate/create
- rollback dijalankan dengan config/schema yang berbeda dari run awal

## Decision Matrix Mode Sync

Gunakan matrix ini sebelum memilih `--mode`.

| Kondisi | Mode direkomendasikan | Kenapa |
| --- | --- | --- |
| Full refresh production Oracle -> PostgreSQL | `truncate_safe` | Load ke staging/backup-aware, validasi sebelum dianggap sukses, rollback tersedia |
| Full refresh kecil non-critical | `truncate` | Lebih sederhana dan cepat, tapi destructive langsung |
| Table besar, dependency banyak, storage cukup | `truncate_safe` | Lebih aman untuk data integrity |
| Perlu minim downtime table read | `swap_safe` | Bisa cutover cepat setelah staging siap, tapi butuh storage lebih |
| RDS/storage sempit | `truncate_safe` atau `truncate` | Hindari `swap` besar karena staging/old table bisa menggandakan storage |
| Insert-only event/log table | `append` | Tidak menghapus data lama |
| Incremental berdasarkan updated_at/numeric key | `incremental_safe` | Ada watermark dan backup sebelum apply |
| Reverse PostgreSQL -> Oracle per 5 menit | `upsert` | Tidak truncate Oracle, update berdasarkan key |
| Reverse full replace ke Oracle | `truncate` atau `delete` | Hanya jika window aman dan owner setuju |
| Tidak ada key yang valid | Jangan pakai `upsert` | Upsert butuh key yang benar |
| Ada LOB yang harus dipakai app | mode sesuai kebutuhan + `--lob stream` | BLOB jadi `bytea`, CLOB jadi `text` |
| Audit schema masih ERROR | Jangan execute kecuali `--force` dengan approval | Struktur target belum aman |

### Mode Detail

#### `truncate_safe`

Pakai untuk production default Oracle -> PostgreSQL.

Contoh:

```bash
ops sync --config config.yaml --direction oracle-to-postgres --tables public.table_name --mode truncate_safe --go
```

Kelebihan:

- rollback tersedia jika backup berhasil dibuat
- rowcount validation wajib
- cocok untuk full refresh

Risiko:

- butuh ruang untuk backup/staging sesuai implementasi table
- lebih lambat dari direct truncate

#### `truncate`

Pakai hanya jika target boleh kosong sementara dan rollback manual diterima.

Contoh:

```bash
ops sync --config config.yaml --direction oracle-to-postgres --tables public.table_name --mode truncate --go
```

Kelebihan:

- cepat
- object table tetap sama, index/grant/dependency tetap melekat

Risiko:

- destructive langsung
- rollback otomatis tidak dijamin

#### `swap_safe`

Pakai jika butuh cutover cepat dan storage cukup.

Contoh:

```bash
ops sync --config config.yaml --direction oracle-to-postgres --tables public.table_name --mode swap_safe --go
```

Kelebihan:

- live table bisa tetap ada saat staging dibuat
- cutover rename relatif cepat

Risiko:

- storage bisa besar
- dependency/grant/index harus diperiksa lebih ketat
- execute swap di-guard oleh config `sync.allow_swap`

#### `append`

Pakai untuk data insert-only.

Contoh:

```bash
ops sync --config config.yaml --direction oracle-to-postgres --tables public.event_log --mode append --go
```

Kelebihan:

- tidak menghapus target
- bisa dipakai untuk log/event

Risiko:

- duplikasi jika source mengirim row yang sama
- missing-key compare sangat disarankan

#### `upsert`

Pakai jika ada key jelas.

Contoh:

```bash
ops sync \
  --config config.yaml \
  --direction postgres-to-oracle \
  --tables public.sample_customer \
  --mode upsert \
  --key-columns customer_id \
  --incremental-column updated_at \
  --incremental \
  --go
```

Kelebihan:

- cocok untuk reverse/incremental
- tidak perlu full truncate

Risiko:

- key salah = data salah update
- butuh unique index/constraint sesuai arah sync
- delete di source tidak otomatis menghapus target

#### `incremental_safe`

Pakai untuk incremental yang perlu rollback protection.

Contoh:

```bash
ops sync \
  --config config.yaml \
  --direction oracle-to-postgres \
  --tables public.table_name \
  --mode incremental_safe \
  --incremental-column updated_at \
  --incremental \
  --go
```

Kelebihan:

- watermark hanya maju setelah run sukses
- rollback state dicatat

Risiko:

- bergantung pada kualitas incremental column
- perlu overlap jika timestamp bisa terlambat masuk

## Decision Matrix Validasi

| Gejala | Command pertama | Command lanjutan |
| --- | --- | --- |
| Rowcount beda | `ops validate --tables ...` | `ops validate missing-keys --tables ...` |
| Rows failed > 0 | buka `failed_row_samples` dan `logs.txt` | retry setelah root cause selesai |
| Checksum mismatch | cek `validation_checksum.csv` | missing-key compare dan cek column/date precision |
| LOB gagal | `ops analyze lob --tables ...` | sync ulang dengan policy `stream`, `skip`, atau `null` |
| Dependency invalid | `ops dependencies check` | `ops dependencies repair` |
| Cron gagal berulang | `ops circuit-breaker list` | rollback/validate, lalu `ops circuit-breaker reset` |
| Incremental salah window | `ops watermarks` | `ops reset-watermark TABLE` dengan approval |

## Cron DBA Checklist

Sebelum pasang cron:

```bash
ops doctor --config config.yaml
ops sync --config config.yaml --profile daily --direction oracle-to-postgres
ops sync --config config.yaml --profile every_5min --direction postgres-to-oracle --mode upsert --tables public.sample_customer --key-columns customer_id --incremental-column updated_at --incremental
```

Pastikan:

- command manual dry-run sukses
- `CONFIG_PATH` di job wrapper benar
- `RETRY` masuk akal
- `TIMEOUT_SECONDS` lebih panjang dari durasi normal
- `LOG_ROTATE_BYTES` dan `LOG_RETENTION_DAYS` terisi
- `ALERT_COMMAND` atau `job.alert` aktif untuk production
- lock file berbeda untuk job yang memang boleh berjalan paralel

Contoh run manual wrapper:

```bash
CONFIG_PATH=/path/to/config.yaml RETRY=3 TIMEOUT_SECONDS=7200 jobs/daily.sh oracle_to_pg
CONFIG_PATH=/path/to/config.yaml RETRY=3 TIMEOUT_SECONDS=900 jobs/incremental.sh pg_to_oracle --tables public.sample_customer --mode upsert --key-columns customer_id --incremental-column updated_at --incremental
```

Ingat: wrapper job menambahkan `--go`, jadi itu execute sungguhan.

### Cron PostgreSQL -> Oracle Per Menit

Untuk table operasional kecil/menengah yang harus reverse sync cepat, gunakan
wrapper lokal `jobs/pg_to_oracle_every_1min.sh`. File ini sengaja masuk
`.gitignore` karena isinya biasanya spesifik environment: daftar table,
unique key, jumlah worker, timeout, dan pilihan dry-run.

Dry-run manual dulu:

```bash
cd /home/lutuk/project/pg2ora2pg/oracle-pg-sync-audit
P2O_1MIN_DRY_RUN=1 \
P2O_1MIN_WORKERS=6 \
CONFIG_PATH=/home/lutuk/project/pg2ora2pg/oracle-pg-sync-audit/config.yaml \
PYTHON_BIN=/home/lutuk/project/pg2ora2pg/oracle-pg-sync-audit/.venv/bin/python \
jobs/pg_to_oracle_every_1min.sh
```

Execute manual setelah dry-run aman:

```bash
cd /home/lutuk/project/pg2ora2pg/oracle-pg-sync-audit
P2O_1MIN_DRY_RUN=0 \
P2O_1MIN_WORKERS=6 \
CONFIG_PATH=/home/lutuk/project/pg2ora2pg/oracle-pg-sync-audit/config.yaml \
PYTHON_BIN=/home/lutuk/project/pg2ora2pg/oracle-pg-sync-audit/.venv/bin/python \
jobs/pg_to_oracle_every_1min.sh
```

Contoh crontab production:

```cron
SHELL=/bin/bash
* * * * * cd /home/lutuk/project/pg2ora2pg/oracle-pg-sync-audit && P2O_1MIN_WORKERS=6 P2O_1MIN_DRY_RUN=0 jobs/pg_to_oracle_every_1min.sh
```

Wrapper sudah punya default untuk `CONFIG_PATH`, `PYTHON_BIN`, `LOG_DIR`,
`LOCK_DIR`, `RETRY`, `TIMEOUT_SECONDS`, `LOG_ROTATE_BYTES`, dan
`LOG_RETENTION_DAYS`, jadi cron cukup satu baris jika repo path dan `.venv`
standar. Tambahkan env di crontab hanya kalau perlu override:

```cron
PYTHON_BIN=/custom/path/python
TIMEOUT_SECONDS=900
```

Untuk uji cron tanpa write ke Oracle, set `P2O_1MIN_DRY_RUN=1` dulu di baris
cron. Tidak perlu `source .venv/bin/activate` jika `PYTHON_BIN` sudah menunjuk
ke `.venv/bin/python`.

Pantau master log terpusat:

```bash
tail -f reports/job_logs/every_1min_pg_to_oracle.log
```

Cron tidak perlu redirect output ke file lain. Wrapper menangkap output detail
dari job paralel, menulis satu ringkasan finish per table ke master log, dan
menyimpan raw log hanya kalau table gagal. Jika perlu menyimpan raw log untuk
semua table saat investigasi, jalankan dengan `P2O_1MIN_KEEP_RAW_LOGS=1`.
Jika perlu melihat event start/retry per table, set `P2O_1MIN_LOG_STARTS=1`.
Lokasi raw log default: `reports/job_logs/every_1min_pg_to_oracle_raw/`.

Catatan penting:

- table dengan kolom timestamp memakai watermark dan overlap
- table tanpa kolom timestamp berjalan key-only tanpa `WHERE`
- delete propagation tidak ikut di job ini; gunakan tombstone/CDC atau workflow
  full mirror terpisah jika delete juga harus disamakan

## Escalation Rules

Escalate ke DBA senior/app owner jika:

- `row_count_diff` bukan nol setelah retry
- missing-key compare menunjukkan key hilang/ekstra
- checksum mismatch berulang
- rollback gagal
- dependency repair gagal
- LOB content dibutuhkan aplikasi tapi target column bukan `bytea`/`text` yang sesuai
- circuit breaker aktif lebih dari satu cooldown
- ada kebutuhan memakai `--force`, `truncate_cascade`, atau direct `truncate` pada table production besar
