"""Load the existing PySpark aggregate outputs into the dashboard's Cassandra tables."""

from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster


BASE_DIR = Path(__file__).resolve().parent
RESULTS_DIR = BASE_DIR / "github-real-results"
DATASET_ID = "current"


def as_count(value: Any) -> int:
    number = pd.to_numeric(value, errors="coerce")
    if pd.isna(number) or number < 0:
        return 0
    return int(number)


def as_date(value: Any) -> date:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"Invalid date in result CSV: {value!r}")
    return parsed.date()


def read_csv(name: str, required: set[str]) -> pd.DataFrame:
    path = RESULTS_DIR / name
    if not path.is_file():
        raise FileNotFoundError(f"Missing {path}. Run the PySpark notebook first.")
    frame = pd.read_csv(path)
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"{path.name} is missing columns: {', '.join(sorted(missing))}")
    return frame


def main() -> None:
    contact_points = [
        host.strip()
        for host in os.getenv("CASSANDRA_CONTACT_POINTS", "127.0.0.1").split(",")
        if host.strip()
    ]
    if not contact_points:
        raise ValueError("Set CASSANDRA_CONTACT_POINTS to one or more Cassandra node addresses.")
    port = int(os.getenv("CASSANDRA_PORT", "9042"))
    keyspace = os.getenv("CASSANDRA_KEYSPACE", "github_analytics")
    username = os.getenv("CASSANDRA_USERNAME")
    password = os.getenv("CASSANDRA_PASSWORD")
    bundle = os.getenv("CASSANDRA_SECURE_CONNECT_BUNDLE")
    if bool(username) != bool(password):
        raise ValueError("Set both CASSANDRA_USERNAME and CASSANDRA_PASSWORD, or leave both unset.")
    auth_provider = PlainTextAuthProvider(username=username, password=password) if username and password else None

    events = read_csv("event_types.csv", {"event_type", "count"})
    repositories = read_csv("repositories.csv", {"repository", "count"})
    developers = read_csv("developers.csv", {"developer", "count"})
    daily = read_csv("daily_activity.csv", {"date", "count"})
    heatmap = read_csv("activity_heatmap.csv", {"date", "hour", "count"})
    summary_path = RESULTS_DIR / "summary.json"
    insights_path = RESULTS_DIR / "insights.txt"
    if not summary_path.is_file() or not insights_path.is_file():
        raise FileNotFoundError("summary.json and insights.txt must exist in github-real-results.")
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    if not isinstance(summary, dict):
        raise ValueError("summary.json must contain a JSON object.")
    insights = insights_path.read_text(encoding="utf-8")

    event_rows = [(DATASET_ID, str(row["event_type"]), as_count(row["count"])) for _, row in events.iterrows()]
    repository_rows = [(DATASET_ID, str(row["repository"]), as_count(row["count"])) for _, row in repositories.iterrows()]
    developer_rows = [(DATASET_ID, str(row["developer"]), as_count(row["count"])) for _, row in developers.iterrows()]
    daily_rows = [
        (DATASET_ID, as_date(row["date"]), as_count(row["count"]))
        for _, row in daily.iterrows()
    ]
    heatmap_rows = []
    for _, row in heatmap.iterrows():
        hour = int(pd.to_numeric(row["hour"], errors="raise"))
        if not 0 <= hour <= 23:
            raise ValueError(f"Invalid hour in activity_heatmap.csv: {hour}")
        heatmap_rows.append((DATASET_ID, as_date(row["date"]), hour, as_count(row["count"])))

    cluster = (
        Cluster(cloud={"secure_connect_bundle": bundle}, auth_provider=auth_provider)
        if bundle
        else Cluster(contact_points, port=port, auth_provider=auth_provider)
    )
    try:
        session = cluster.connect(keyspace)
        # Replace only this dashboard's aggregate partition so reruns do not leave stale labels.
        for table in ("event_types", "repositories", "developers", "daily_activity", "activity_heatmap"):
            session.execute(f"DELETE FROM {table} WHERE dataset_id = %s", (DATASET_ID,))

        statements = {
            "event_types": session.prepare(
                "INSERT INTO event_types (dataset_id, event_type, event_count) VALUES (?, ?, ?)"
            ),
            "repositories": session.prepare(
                "INSERT INTO repositories (dataset_id, repository, event_count) VALUES (?, ?, ?)"
            ),
            "developers": session.prepare(
                "INSERT INTO developers (dataset_id, developer, event_count) VALUES (?, ?, ?)"
            ),
            "daily_activity": session.prepare(
                "INSERT INTO daily_activity (dataset_id, activity_date, event_count) VALUES (?, ?, ?)"
            ),
            "activity_heatmap": session.prepare(
                "INSERT INTO activity_heatmap (dataset_id, activity_date, hour, event_count) VALUES (?, ?, ?, ?)"
            ),
            "summary": session.prepare("INSERT INTO summary (dataset_id, payload) VALUES (?, ?)"),
            "insights": session.prepare("INSERT INTO insights (dataset_id, content) VALUES (?, ?)"),
        }
        row_sets = {
            "event_types": event_rows,
            "repositories": repository_rows,
            "developers": developer_rows,
            "daily_activity": daily_rows,
            "activity_heatmap": heatmap_rows,
        }
        loaded = {}
        for table, rows in row_sets.items():
            for values in rows:
                session.execute(statements[table], values)
            loaded[table] = len(rows)
        session.execute(statements["summary"], (DATASET_ID, json.dumps(summary, ensure_ascii=False)))
        session.execute(statements["insights"], (DATASET_ID, insights))
        print(f"Loaded dashboard aggregates into keyspace '{keyspace}' (dataset_id='{DATASET_ID}'):")
        for table, count in loaded.items():
            print(f"  {table}: {count} rows")
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()
