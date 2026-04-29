# Oracle Client Install

Use this only when your environment requires Oracle Thick mode. This project can keep Oracle Instant Client inside the repo workspace so it does not depend on `/opt/oracle/...` existing on every machine.

## Recommended Layout

```text
oracle-pg-sync-audit/
  vendor/
    oracle/
      instantclient_23_26/
```

`python-oracledb` only needs the `Basic` package for Thick mode.

## Official Download

Oracle publishes a permanent Linux x86-64 Basic ZIP link:

```text
https://download.oracle.com/otn_software/linux/instantclient/instantclient-basic-linuxx64.zip
```

Oracle download pages and install docs:

- https://www.oracle.com/sg/database/technologies/instant-client/linux-x86-64-downloads.html
- https://docs.oracle.com/en/database/oracle/oracle-database/23/lacli/installing-instant-client.html

## Download Into This Project

If `curl` or `wget` is not installed, use Python:

```bash
cd /home/lutuk/project/pg2ora2pg/oracle-pg-sync-audit
python3 -c "import urllib.request, pathlib; pathlib.Path('vendor/oracle').mkdir(parents=True, exist_ok=True); urllib.request.urlretrieve('https://download.oracle.com/otn_software/linux/instantclient/instantclient-basic-linuxx64.zip', 'vendor/oracle/instantclient-basic-linuxx64.zip')"
```

Extract it:

```bash
cd /home/lutuk/project/pg2ora2pg/oracle-pg-sync-audit
python3 -c "import zipfile; zipfile.ZipFile('vendor/oracle/instantclient-basic-linuxx64.zip').extractall('vendor/oracle')"
```

This creates a folder like:

```text
vendor/oracle/instantclient_23_26
```

## Configure The Project

Set `.env`:

```dotenv
ORACLE_CLIENT_LIB_DIR=vendor/oracle/instantclient_23_26
```

Keep this in `config.yaml`:

```yaml
oracle:
  client_lib_dir: ${ORACLE_CLIENT_LIB_DIR}
```

The config loader resolves relative paths against the config file location, so `vendor/oracle/instantclient_23_26` works without an absolute machine-specific path.

## Runtime Behavior

At startup, the CLI checks `oracle.client_lib_dir`. If it exists, the process re-execs with `LD_LIBRARY_PATH` including that directory so `python-oracledb` can initialize Thick mode cleanly.

## Verify

Run:

```bash
ops doctor --config config.yaml
```

If Oracle connectivity still fails, verify:

- the extracted folder exists on disk
- host architecture matches the client package
- Oracle network configuration and credentials are correct
- the downloaded package is a Linux x86-64 Instant Client Basic package

## Notes

- The repo `.gitignore` excludes `vendor/oracle/*.zip` and extracted Instant Client folders by default.
- Keep the client local to the workspace or distribute it separately in deployment automation; do not rely on `/opt/oracle/...` existing everywhere.
