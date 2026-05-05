# Panduan Operator Awam

Panduan ini untuk menjalankan audit dan sync tanpa perlu membaca detail kode.
Gunakan command `ops` sebagai command utama.

## Prinsip Aman

- `.env` otomatis dibaca. Tidak perlu `export` manual.
- `sync` default-nya dry-run. Data baru berubah kalau pakai `--go`.
- Untuk cek satu run, selalu buka folder `reports/run_<timestamp>_<run_id>/`.
- Pakai `logs.txt` di folder run tersebut. Jangan pakai `reports/sync.log` untuk analisa satu run karena itu log global.
- Kalau hasil validasi `MISMATCH`, `FAILED`, atau `row_count_match=false`, jangan anggap sukses.

## 1. Cek Koneksi

```bash
ops doctor --config config.yaml
```

Hasil yang bagus biasanya berisi:

```text
env_loaded,OK
postgres_connection,OK
oracle_connection,OK
```

Kalau ada DNS/host error, jalankan ulang setelah koneksi stabil. Tool sudah punya retry untuk koneksi, tapi DNS yang benar-benar down tetap harus dibetulkan dari jaringan/VPN/DNS.

## 2. Audit Sebelum Sync

Untuk satu table:

```bash
ops audit --config config.yaml --tables public.nama_table --exact-count
```

Untuk semua table di config:

```bash
ops audit --config config.yaml --exact-count
```

Buka:

```text
reports/run_<timestamp>_<run_id>/report.html
reports/run_<timestamp>_<run_id>/report.xlsx
reports/run_<timestamp>_<run_id>/logs.txt
```

Cek bagian penting:

- `Rowcount Mismatch`
- `Column Diff`
- `Dependency Summary`
- `LOB Summary`

Catatan: kalau log audit tidak menampilkan `where=...`, artinya full-table audit. `where` hanya muncul jika memang ada filter.

## 3. Dry-Run Sync

Dry-run tidak mengubah data:

```bash
ops sync \
  --config config.yaml \
  --direction oracle-to-postgres \
  --tables public.nama_table \
  --mode truncate_safe
```

Untuk table dengan BLOB/CLOB yang memang harus bisa dibuka oleh aplikasi, gunakan LOB stream/include:

```bash
ops sync \
  --config config.yaml \
  --direction oracle-to-postgres \
  --tables public.nama_table \
  --mode truncate_safe \
  --lob stream
```

`stream` menyimpan BLOB Oracle ke PostgreSQL sebagai `bytea`, sehingga aplikasi bisa membaca isi binary-nya selama kolom target dan aplikasi memang memakai tipe tersebut.

## 4. Execute Sync

Jalankan hanya setelah audit dan dry-run sudah jelas:

```bash
ops sync \
  --config config.yaml \
  --direction oracle-to-postgres \
  --tables public.nama_table \
  --mode truncate_safe \
  --go
```

Untuk LOB:

```bash
ops sync \
  --config config.yaml \
  --direction oracle-to-postgres \
  --tables public.nama_table \
  --mode truncate_safe \
  --lob stream \
  --go
```

Mode paling aman untuk produksi biasanya `truncate_safe`, karena data dimuat ke staging dulu lalu divalidasi sebelum target final diganti.

## 5. Validasi Setelah Sync

Rowcount:

```bash
ops validate --config config.yaml --tables public.nama_table
```

Missing/extra key. Jika `key_columns` belum diset, CLI akan mencoba `PRIMARY KEY` lalu `UNIQUE` constraint dari Oracle/PostgreSQL:

```bash
ops validate missing-keys --config config.yaml --tables public.nama_table
```

Output missing key:

```text
missing_keys_summary.csv
keys_in_oracle_not_in_postgres.csv
keys_in_postgres_not_in_oracle.csv
```

`missing-keys` melakukan full sorted streaming compare. Artinya status `MATCH/MISMATCH` tidak hanya berdasarkan sample awal; sample hanya membatasi jumlah detail yang ditulis ke CSV.

## 6. Cara Membaca Status

- `SUCCESS`: sync selesai dan validasi wajib lolos.
- `MATCH`: audit/validasi cocok.
- `WARNING`: ada perbedaan ringan atau hal yang perlu dicek, belum tentu gagal.
- `MISMATCH`: ada beda penting, wajib review.
- `FAILED`: proses gagal.
- `MISSING`: object/data yang dibutuhkan tidak ditemukan.

Field penting di `sync_result.csv`:

```text
rows_read_from_oracle
rows_written_to_postgres
rows_failed
oracle_row_count
postgres_row_count
row_count_match
row_count_diff
validation_status
data_integrity_status
```

`data_integrity_status`:

- `PASS`: copy selesai, rowcount valid, checksum tidak mismatch, dan tidak ada failed rows.
- `FAIL`: rowcount mismatch, checksum mismatch, row copy tidak lengkap, atau `rows_failed > 0`.
- `UNKNOWN`: copy selesai tapi validasi wajib belum lengkap, sehingga status table tidak boleh dianggap bersih.

## 7. Jika Ada Masalah

DNS/host error:

```bash
ops doctor --config config.yaml
```

Rowcount mismatch:

```bash
ops validate --config config.yaml --tables public.nama_table
ops validate missing-keys --config config.yaml --tables public.nama_table
```

LOB error:

```bash
ops analyze lob --config config.yaml --tables public.nama_table
```

Lihat `failed_row_samples` dan `logs.txt` pada folder run.

Dependency error:

```bash
ops dependencies check --config config.yaml
ops dependencies repair --config config.yaml
```

Job gagal berulang atau cron berhenti karena circuit breaker:

```bash
ops circuit status --config config.yaml
ops circuit reset "JOB_KEY_DARI_STATUS" --config config.yaml
```

Reset circuit hanya setelah root cause sudah jelas, data sudah divalidasi atau
di-rollback, dan dry-run berikutnya bersih.

Rollback, jika run memakai safe mode dan backup tersedia:

```bash
ops rollback <run_id> --config config.yaml
```

## Checklist Sebelum `--go`

- `ops doctor` OK.
- Audit table sudah dicek.
- Tidak ada rowcount mismatch yang belum dijelaskan.
- LOB strategy sudah jelas: `error`, `skip`, `null`, atau `stream`.
- Untuk upsert/incremental, `key_columns` sudah benar.
- Folder run terbaru sudah dibuka dan `logs.txt` milik run itu sendiri.

## Checklist Sebelum Pasang Cron

- Jalankan manual dulu command yang sama tanpa cron.
- Pastikan `jobs/daily.sh`, `jobs/incremental.sh`, dan `jobs/every_5min.sh` executable.
- Pastikan `CONFIG_PATH` menunjuk ke config production yang benar.
- Isi `RETRY`, `TIMEOUT_SECONDS`, `LOG_ROTATE_BYTES`, dan `LOG_RETENTION_DAYS` dengan angka.
- Untuk job incremental reverse, selalu isi `--mode upsert`, `--key-columns`, dan `--incremental-column`.
- Ingat: job wrapper menambahkan `--go`, jadi cron berarti execute sungguhan.

## Catatan PostgreSQL ke Oracle

Untuk arah reverse, mode harian yang paling umum adalah `upsert`:

```bash
ops sync \
  --config config.yaml \
  --direction postgres-to-oracle \
  --tables public.nama_table \
  --mode upsert \
  --key-columns id \
  --incremental-column last_update \
  --incremental
```

Validasi setelah execute:

```bash
ops validate --config config.yaml --direction postgres-to-oracle --tables public.nama_table
ops validate missing-keys --config config.yaml --direction postgres-to-oracle --tables public.nama_table
```

Jangan pakai `upsert` kalau key belum jelas. Untuk reverse full replace,
gunakan `--mode truncate` hanya saat window aman dan app owner setuju Oracle
target boleh diganti penuh.

Panduan operasional DBA yang lebih detail ada di
[DBA Daily Operations Guide](DBA_DAILY_OPERATIONS.md).
