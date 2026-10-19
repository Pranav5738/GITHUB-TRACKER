# GitHub Public Activity Analytics

## Run with local result files

```powershell
python -m pip install -r requirements.txt
python -m streamlit run app.py
```

The dashboard uses `github-real-results` when Cassandra is not configured.

## Run with live public GitHub events

Start the same Streamlit app, then choose **Live public events** in the sidebar.
The live view polls GitHub's public Events API, displays recent event types and
repositories, and updates counts during the browser session. Auto refresh is on
by default; polling respects GitHub's `X-Poll-Interval` response header and has
a minimum interval of 60 seconds. Public event delivery can be delayed, so this
is near-real-time activity rather than a complete or instantaneous feed. Events
observed in the live view are kept in browser session state and are not appended
to the project's JSONL file or Cassandra tables.

An optional `GITHUB_TOKEN` can be set in the project-root `.env` file (created
with this project) to use an authenticated API quota. Paste the token after the
equals sign. Keep `.env` private and do not commit or share it. The view works
without a token within GitHub's unauthenticated API limits.

```powershell
notepad .env
# Add your token as: GITHUB_TOKEN=github_pat_...
python -m streamlit run app.py
```

## Run with Cassandra

The app reads pre-aggregated PySpark result tables from Cassandra. It does not
scan or reprocess the raw GitHub events. The CQL schema is in
`cassandra_schema.cql`; the checked-in replication setting is for a local,
single-node project setup.

1. Start Docker Desktop and make sure `docker info` works. The Cassandra port
   must be published to the Windows host because Streamlit and the loader run
   outside the Cassandra container:

   ```powershell
   docker network create cassandra
   docker run --detach --name cassandra --hostname cassandra --network cassandra --publish 9042:9042 --restart unless-stopped cassandra
   ```

   If the `cassandra` Docker network already exists, skip the `docker network
   create` command. Wait for the container to finish starting before continuing.

2. Apply the project schema inside the container:

   ```powershell
   docker cp .\cassandra_schema.cql cassandra:/tmp/cassandra_schema.cql
   docker exec cassandra cqlsh -f /tmp/cassandra_schema.cql
   ```

3. Configure the connection in the current PowerShell session. For the local
   Cassandra node above, with authentication disabled:

   ```powershell
   $env:CASSANDRA_CONTACT_POINTS = "127.0.0.1"
   $env:CASSANDRA_PORT = "9042"
   $env:CASSANDRA_KEYSPACE = "github_analytics"
   ```

   For an authenticated cluster, also set `CASSANDRA_USERNAME` and
   `CASSANDRA_PASSWORD`. For Astra, set `CASSANDRA_SECURE_CONNECT_BUNDLE` to
   the secure-connect bundle path and set the username and password.

4. Load the existing generated results into the `current` dataset partition:

   ```powershell
   python load_results_to_cassandra.py
   ```

   The loader replaces only this dashboard's aggregate rows. It does not
   modify the notebook or raw JSONL file.

5. Start Streamlit:

   ```powershell
   python -m streamlit run app.py
   ```

   In the sidebar, choose **Automatic** (uses Cassandra when configured) or
   **Cassandra**. **Local result files** keeps the original CSV-backed mode.

Connection values can also be stored in `.streamlit/secrets.toml` under a
`[cassandra]` section (`contact_points`, `port`, `keyspace`, `username`,
`password`, and optionally `secure_connect_bundle`). Do not commit credentials
or secure-connect bundles.

The Cassandra tables are query-oriented aggregate tables, matching the
dashboard sections. This dataset is small; production clusters should use a
replication strategy appropriate to their topology instead of the local
`SimpleStrategy` setting in the sample schema.



commands to run


docker cp .\cassandra_schema.cql cassandra:/tmp/cassandra_schema.cql
docker exec cassandra cqlsh -f /tmp/cassandra_schema.cql

$env:CASSANDRA_CONTACT_POINTS = "127.0.0.1"
$env:CASSANDRA_PORT = "9042"
$env:CASSANDRA_KEYSPACE = "github_analytics"

python load_results_to_cassandra.py
python -m streamlit run app.py
