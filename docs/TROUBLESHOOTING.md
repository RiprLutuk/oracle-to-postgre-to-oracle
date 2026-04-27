# Troubleshooting

## ModuleNotFoundError: oracledb

Install dependency:

```bash
pip install -r requirements.txt
```

Atau:

```bash
pip install oracledb
```

## ModuleNotFoundError: psycopg

Install dependency:

```bash
pip install -r requirements.txt
```

## DPI-1047 atau Oracle Client Library Tidak Ketemu

Jika memakai thick mode, pastikan Oracle Instant Client terinstall dan `.env` berisi:

```dotenv
ORACLE_CLIENT_LIB_DIR=/opt/oracle/instantclient_23_9
```

Pastikan folder tersebut berisi library Oracle Client.

## ORA-01017 Invalid Username/Password

Cek:

- `ORACLE_USER`
- `ORACLE_PASSWORD`
- service yang dituju benar
- user tidak locked

## ORA-12154 atau ORA-12514

Cek:

- `ORACLE_DSN`
- `ORACLE_HOST`
- `ORACLE_PORT`
- `ORACLE_SERVICE_NAME`
- Listener Oracle mengenali service name tersebut.

Tes network:

```bash
nc -vz $ORACLE_HOST $ORACLE_PORT
```

## PostgreSQL Connection Refused

Cek:

- `PG_HOST`
- `PG_PORT`
- firewall/security group
- PostgreSQL listen address

Tes:

```bash
nc -vz $PG_HOST $PG_PORT
```

## Permission Denied for Schema

User PostgreSQL butuh privilege schema:

```sql
GRANT USAGE, CREATE ON SCHEMA public TO sync_user;
```

## Permission Denied for Table

Grant table privilege:

```sql
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA public TO sync_user;
```

## COPY Gagal Karena Type Mismatch

Kemungkinan:

- PostgreSQL column terlalu kecil.
- Oracle `NUMBER` berisi decimal tapi PG integer.
- Bytea/text tidak cocok.
- Ada karakter null `\x00` di string.

Langkah:

1. Jalankan audit.
2. Buka `type_mismatch.csv`.
3. Perbaiki DDL PostgreSQL atau rename mapping.
4. Ulang dry-run.

## Sync Table Di-Skip

Jika status `SKIPPED`, biasanya karena mismatch fatal:

- table missing
- missing column
- extra column
- type mismatch

Cek:

```text
reports/inventory_summary.csv
reports/column_diff.csv
reports/type_mismatch.csv
reports/sync_result.csv
```

Jika tetap ingin lanjut:

```bash
python -m oracle_pg_sync sync --config config.yaml --tables ADDRESS --mode swap --execute --force
```

Gunakan `--force` hanya setelah review.

## Rowcount Tidak Match

Penyebab umum:

- Ada data berubah saat sync berjalan.
- Audit memakai fast count/statistik lama.
- `where` filter aktif di table config.
- Ada trigger/rule di PostgreSQL yang mengubah hasil insert.
- Ada duplicate/constraint behavior saat upsert.

Langkah:

```bash
python -m oracle_pg_sync audit --config config.yaml --tables ADDRESS --exact-count
```

Jika masih mismatch, cek manual:

```sql
SELECT COUNT(1) FROM public.sample_customer;
```

Dan di Oracle:

```sql
SELECT COUNT(1) FROM APP_SCHEMA.SAMPLE_CUSTOMER;
```

## Swap Gagal Karena Lock Timeout

Penyebab:

- Aplikasi sedang query/transaction panjang di table target.
- Job lain memegang lock.

Cek PostgreSQL:

```sql
SELECT pid, state, wait_event_type, wait_event, query
FROM pg_stat_activity
WHERE datname = current_database()
ORDER BY query_start NULLS LAST;
```

Solusi:

- Jalankan di maintenance window.
- Hentikan transaction panjang setelah approval.
- Ulang sync table tersebut.

## Upsert Gagal: No Unique Constraint

Mode `upsert` butuh unique index/constraint sesuai `key_columns`.

Contoh:

```sql
CREATE UNIQUE INDEX CONCURRENTLY sample_customer_uq
ON public.sample_customer (customer_id);
```

Lalu config:

```yaml
tables:
  - name: public.sample_customer
    mode: upsert
    key_columns: [customer_id]
```

## Reverse Sync PostgreSQL ke Oracle Gagal Saat Upsert

Mode reverse `upsert` memakai Oracle `MERGE`. Pastikan:

- `key_columns` diisi.
- Key column ada di Oracle target.
- Data source tidak menghasilkan duplicate untuk key yang sama.
- User Oracle punya privilege insert/update.

Contoh:

```yaml
tables:
  - name: public.sample_customer
    mode: upsert
    key_columns: [CUSTOMER_ID]
```

Command:

```bash
python -m oracle_pg_sync sync --config config.yaml --direction postgres-to-oracle --tables sample_customer --mode upsert --execute
```

## Reverse Sync Swap Di-Skip

Ini by design. `swap` ke Oracle tidak diaktifkan karena dapat mengganggu grants, views, triggers, synonyms, dan dependency. Gunakan `truncate`, `delete`, `append`, atau `upsert`.

## report.html Kosong

Kemungkinan CSV belum dibuat. Jalankan audit dulu:

```bash
python -m oracle_pg_sync audit --config config.yaml
python -m oracle_pg_sync report --config config.yaml
```

## Excel Tidak Terbuat

Pastikan dependency:

```bash
pip install pandas openpyxl
```

Atau reinstall:

```bash
pip install -r requirements.txt
```

## Debug Log Lebih Detail

Tambahkan `--verbose`:

```bash
python -m oracle_pg_sync --verbose audit --config config.yaml
```

Atau:

```bash
python -m oracle_pg_sync audit --config config.yaml --verbose
```
