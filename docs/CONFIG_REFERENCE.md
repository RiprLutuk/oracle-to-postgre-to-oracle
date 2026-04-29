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
- Contoh: `APP_SCHEMA`.

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
- Default project: `truncate`, supaya object existing tetap aman dan tidak membuat staging table besar.

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

`allow_swap`

- Default `false`.
- Jika `false`, execute mode `swap` akan di-skip kecuali command memakai `--force`.
- Ini guard untuk RDS karena `swap` membuat staging table, index staging, WAL/temp, dan old table selama transaksi.

`max_swap_table_bytes`

- Batas ukuran table PostgreSQL untuk mode `swap`.
- Bisa angka byte atau string seperti `20GiB`.
- Jika ukuran table melewati batas, mode `swap` di-skip kecuali memakai `--force`.

`swap_space_multiplier`

- Multiplier estimasi storage ekstra untuk dry-run/log swap.
- Default `2.5`, karena staging table plus index/WAL/temp bisa lebih besar dari data heap saja.

`keep_old_after_swap`

- Jika `true`, table lama hasil swap tidak langsung dihapus.
- Rekomendasi default: `false` agar storage RDS cepat balik setelah swap.
- Jika butuh rollback cepat via old table, aktifkan hanya saat free storage cukup.

`copy_null`

- Placeholder untuk format null COPY.

## dependency

`auto_recompile_oracle`

- Jika `true`, execute sync menjalankan compile invalid object Oracle setelah load.

`refresh_postgres_mview`

- Jika `true`, execute sync menjalankan refresh materialized view PostgreSQL
  yang terdeteksi dependent.

`max_recompile_attempts`

- Batas loop compile invalid object Oracle.
- Default `3`.

`fail_on_broken_dependency`

- Jika `true`, run execute keluar non-zero bila dependency critical masih
  broken, invalid, missing, atau failed setelah dependency lifecycle.

`pg_lock_timeout`

- PostgreSQL lock timeout untuk action yang butuh lock, terutama `truncate`.
- Default `5s`.
- Jika table sedang dipakai dan lock tidak didapat, proses gagal cepat.

`pg_statement_timeout`

- PostgreSQL statement timeout.
- Default `0`, artinya tidak dibatasi.

`checkpoint_dir`

- Lokasi SQLite checkpoint.
- Default: `reports/checkpoints/checkpoint.sqlite3`.

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
  allow_swap: false
  max_swap_table_bytes: 20GiB
  swap_space_multiplier: 2.5
  keep_old_after_swap: false
  pg_lock_timeout: 5s
  pg_statement_timeout: '0'
  checkpoint_dir: reports/checkpoints/checkpoint.sqlite3
```

## Table List dan Runtime Override

Untuk production yang mudah dibaca, table list bisa dibuat simple:

```yaml
tables_file: configs/tables.yaml
```

```yaml
# configs/tables.yaml
tables:
  - public.address
  - public.housemaster
  - public.a_hp_house_info
```

Detail yang berubah per job, terutama PostgreSQL ke Oracle, bisa diisi di command:

```bash
ops sync \
  --direction postgres-to-oracle \
  --tables public.address \
  --mode upsert \
  --key-columns address_id \
  --incremental-column last_update \
  --where "last_update >= CURRENT_TIMESTAMP - INTERVAL '5 minutes'" \
  --incremental \
  --go
```

## Incremental, Checksum, dan LOB

Table-level incremental tetap didukung jika ingin disimpan di YAML:

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

`strategy` dapat berisi `updated_at`, `numeric_key`, atau `oracle_scn`.
`oracle_scn` saat ini akan gagal dengan pesan jelas karena Flashback/SCN belum diaktifkan.

Table list sebaiknya hanya di satu tempat. Untuk production, gunakan:

```yaml
tables_file: configs/tables.yaml
```

Jangan isi `tables:` inline bersamaan dengan `tables_file`; loader akan menolak kombinasi itu agar tidak ada dua sumber table yang berbeda.

File contoh table list ada di:

```text
configs/tables.example.yaml
```

Contoh default dibuat simple. Field lanjutan seperti `where`, `key_columns`,
`incremental`, checksum chunk, dan `lob_strategy` tetap valid jika memang ingin
disimpan di YAML.

Checksum validation:

```yaml
validation:
  checksum:
    enabled: true
    mode: chunk
    batch_size: 5000
    columns: auto
    exclude_columns:
      - BLOB_PAYLOAD
    sample_percent: 1
```

LOB strategy:

```yaml
lob_strategy:
  default: error
  stream_batch_size: 100
  lob_chunk_size_bytes: 1048576
  validation:
    default: size
    hash_algorithm: sha256
  warn_on_lob_larger_than_mb: 50
  fail_on_lob_larger_than_mb: null

tables:
  - name: public.sample_blob_table
    lob_strategy:
      columns:
        BLOB_PAYLOAD:
          strategy: stream
          target_type: bytea
          validation: size_hash
        JSON_DATA:
          strategy: stream
          target_type: text
          validation: size_hash
```

Pilihan LOB: `skip`, `null`, `stream`, `include`, `error`.

Tipe Oracle LOB yang didukung:

- `BLOB` -> PostgreSQL `bytea`
- `CLOB` dan `NCLOB` -> PostgreSQL `text`
- `LONG` -> PostgreSQL `text` jika driver/table mengizinkan pembacaan
- `LONG RAW` -> PostgreSQL `bytea` jika driver/table mengizinkan pembacaan

Default tetap aman: `error`. Nilai LOB mentah tidak ditulis ke log/report/manifest.

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
- Bisa diisi runtime dengan `--key-columns col1 col2`.

`where`

- Filter query Oracle.
- Ditambahkan langsung setelah `WHERE`.
- Pastikan aman dan valid untuk Oracle SQL.
- Bisa diisi runtime dengan `--where`, hanya untuk satu table per command.

## Contoh Config Development

```yaml
sync:
  default_direction: oracle-to-postgres
  default_mode: truncate
  dry_run: true
  fast_count: true
  exact_count_after_load: false
  parallel_workers: 1
  allow_swap: false
  keep_old_after_swap: false
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
  allow_swap: false
  max_swap_table_bytes: 20GiB
  swap_space_multiplier: 2.5
  keep_old_after_swap: false
  pg_lock_timeout: 5s
```
