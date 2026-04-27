# Configuration Reference

Project memakai dua sumber config:

- `.env` untuk secret dan koneksi.
- `config.yaml` untuk opsi runtime, daftar table, mode sync, rename mapping, dan output report.

## .env

`ORACLE_DSN`

- DSN Oracle lengkap.
- Jika diisi, field host/port/service/sid tidak dipakai untuk membuat DSN.
- Contoh: `oracle-host.example.com:1521/ORCLPDB1`.

`ORACLE_HOST`

- Host atau IP Oracle.
- Dipakai jika `ORACLE_DSN` kosong.

`ORACLE_PORT`

- Port listener Oracle.
- Umumnya `1521`.

`ORACLE_SERVICE_NAME`

- Service name Oracle.
- Gunakan ini untuk PDB/service modern.

`ORACLE_SID`

- SID Oracle.
- Isi salah satu antara `ORACLE_SERVICE_NAME` atau `ORACLE_SID`.

`ORACLE_USER`

- User Oracle untuk membaca metadata dan data.

`ORACLE_PASSWORD`

- Password Oracle.

`ORACLE_SCHEMA`

- Owner schema Oracle sumber.
- Contoh: `PRD_AMSPBRIM`.

`ORACLE_CLIENT_LIB_DIR`

- Path Oracle Instant Client jika memakai thick mode.
- Contoh: `/opt/oracle/instantclient_23_9`.

`PG_HOST`

- Host atau IP PostgreSQL.

`PG_PORT`

- Port PostgreSQL.
- Umumnya `5432`.

`PG_DATABASE`

- Database target PostgreSQL.

`PG_USER`

- User PostgreSQL untuk audit dan sync.

`PG_PASSWORD`

- Password PostgreSQL.

`PG_SCHEMA`

- Schema default PostgreSQL.
- Default: `public`.

## config.yaml Root

`env_file`

- File env yang diload sebelum placeholder `${...}` dibaca.
- Default contoh: `.env`.

```yaml
env_file: .env
```

## oracle

```yaml
oracle:
  dsn: ${ORACLE_DSN}
  host: ${ORACLE_HOST}
  port: ${ORACLE_PORT}
  service_name: ${ORACLE_SERVICE_NAME}
  sid: ${ORACLE_SID}
  user: ${ORACLE_USER}
  password: ${ORACLE_PASSWORD}
  schema: ${ORACLE_SCHEMA}
  client_lib_dir: ${ORACLE_CLIENT_LIB_DIR}
```

Gunakan placeholder agar password tidak hardcode.

## postgres

```yaml
postgres:
  host: ${PG_HOST}
  port: ${PG_PORT}
  database: ${PG_DATABASE}
  user: ${PG_USER}
  password: ${PG_PASSWORD}
  schema: ${PG_SCHEMA:-public}
```

Syntax `${PG_SCHEMA:-public}` berarti kalau env kosong, pakai `public`.

## sync

`default_mode`

- Mode default jika table tidak punya mode.
- Nilai: `truncate`, `swap`, `append`, `upsert`.
- Default project: `truncate`, supaya object existing seperti index, trigger, grants, view/materialized view dependency tetap aman dan tidak membuat staging table besar.

`default_direction`

- Arah sync default.
- Nilai: `oracle-to-postgres` atau `postgres-to-oracle`.
- Default: `oracle-to-postgres`.

`dry_run`

- Default harus `true`.
- Command sync tetap tidak mengubah data tanpa `--execute`.

`fast_count`

- `true`: memakai statistik database.
- `false`: memakai exact count.
- Fast count lebih ringan tapi bisa tidak real-time.

`exact_count_after_load`

- Setelah sync, hitung exact count Oracle dan PostgreSQL.
- Bisa berat untuk table besar.
- Default `false` agar tidak memberatkan server. Pakai `--exact-count` saat audit/verifikasi terjadwal.

`parallel_workers`

- Jumlah table yang diproses paralel.
- Default `1` agar tidak memberatkan Oracle/PostgreSQL/client.

`batch_size`

- Ukuran batch konseptual untuk fetch/load.
- Saat ini loader utama memakai cursor iterator dan PostgreSQL COPY.

`chunk_size`

- Disiapkan untuk pengembangan chunking big table.

`skip_on_structure_mismatch`

- Guard agar table mismatch di-skip.
- Behavior utama juga dikontrol oleh `--force`.

`build_indexes_on_staging`

- Disiapkan untuk optimasi index staging.
- Saat ini staging dibuat `LIKE INCLUDING ALL`.

`analyze_after_load`

- Jalankan `ANALYZE` setelah load.

`truncate_cascade`

- Jika `true`, truncate memakai `CASCADE`.
- Jangan aktifkan tanpa approval DBA.

`keep_old_after_swap`

- Jika `true`, table lama hasil swap tidak langsung dihapus.
- Rekomendasi production: `true`.

`copy_null`

- Placeholder untuk format null COPY.

`pg_lock_timeout`

- PostgreSQL lock timeout untuk action yang butuh lock, terutama `truncate`.
- Default `5s`.
- Jika table sedang dipakai dan lock tidak didapat, proses gagal cepat.

`pg_statement_timeout`

- PostgreSQL statement timeout.
- Default `0`, artinya tidak dibatasi.

Contoh:

```yaml
sync:
  default_direction: oracle-to-postgres
  default_mode: truncate
  dry_run: true
  fast_count: true
  exact_count_after_load: false
  parallel_workers: 1
  truncate_cascade: false
  keep_old_after_swap: true
  pg_lock_timeout: 5s
  pg_statement_timeout: '0'
```

## reports

`output_dir`

- Folder output report.
- Default: `reports`.

```yaml
reports:
  output_dir: reports
```

## rename_columns

Mapping kolom Oracle ke kolom PostgreSQL.

```yaml
rename_columns:
  public.sample_customer:
    legacy_status: status
```

Aturan:

- Key table harus lowercase atau case-insensitive equivalent.
- Kolom kiri adalah Oracle.
- Kolom kanan adalah PostgreSQL.
- Dipakai oleh audit dan sync.

## tables

```yaml
tables:
  - name: public.sample_order
    oracle_to_postgres_mode: truncate
    postgres_to_oracle_mode: truncate
    directions: [oracle-to-postgres, postgres-to-oracle]
    key_columns: [order_id]
    where: "UPDATED_AT >= SYSDATE - 1"
```

`name`

- Nama table target.
- Bisa `schema.table` atau `table`.

`mode`

- Override mode generic untuk table itu.
- Dipertahankan untuk config sederhana/backward compatible.
- Jika ada mode per arah, field per arah lebih diprioritaskan.

`oracle_to_postgres_mode`

- Mode khusus saat `--direction oracle-to-postgres`.
- Nilai: `truncate`, `swap`, `append`, `upsert`.
- Default table memakai `truncate` untuk menghindari staging table besar dan menjaga dependency object existing.

`postgres_to_oracle_mode`

- Mode khusus saat `--direction postgres-to-oracle`.
- Nilai: `truncate`, `append`, `delete`, `upsert`.
- Mode `swap` tidak diaktifkan untuk Oracle target.

`directions`

- Arah sync yang valid untuk table tersebut.
- Jika command sync tidak diberi `--tables`, CLI hanya mengambil table yang memiliki direction sesuai.
- Contoh: `[oracle-to-postgres]`, `[postgres-to-oracle]`, atau keduanya.

`key_columns`

- Wajib untuk `upsert`.
- Harus sesuai unique index/constraint di PostgreSQL.
- Untuk PostgreSQL ke Oracle, key dipakai oleh Oracle `MERGE`.

`where`

- Filter query Oracle.
- Ditambahkan langsung setelah `WHERE`.
- Pastikan aman dan valid untuk Oracle SQL.

## Contoh Config Development

```yaml
sync:
  default_direction: oracle-to-postgres
  default_mode: truncate
  dry_run: true
  fast_count: true
  exact_count_after_load: false
  parallel_workers: 1
  keep_old_after_swap: true
```

## Contoh Config Production

```yaml
sync:
  default_direction: oracle-to-postgres
  default_mode: truncate
  dry_run: true
  fast_count: true
  exact_count_after_load: false
  parallel_workers: 1
  analyze_after_load: true
  truncate_cascade: false
  keep_old_after_swap: true
  pg_lock_timeout: 5s
```
