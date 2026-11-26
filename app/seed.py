"""Built-in deployment records schema + deterministic seed data."""
from __future__ import annotations

import datetime as dt
from typing import Any

SEED_SQL = """
CREATE TABLE IF NOT EXISTS deployments (
    id           INTEGER PRIMARY KEY,
    service      TEXT NOT NULL,
    version      TEXT NOT NULL,
    environment  TEXT NOT NULL,
    status       TEXT NOT NULL,
    deployed_at  TIMESTAMP NOT NULL,
    deployed_by  TEXT NOT NULL
);
"""

# Deterministic records around 2026-10 / 2026-11 so every date/service/version
# filter has something to return.
_SERVICES = ["api-gateway", "auth-service", "report-worker", "web-frontend"]
_VERSIONS = {
    "api-gateway": ["v2.0.1", "v2.0.0", "v1.9.2"],
    "auth-service": ["v3.1.0", "v3.0.4", "v2.9.9"],
    "report-worker": ["v1.8.3", "v1.8.2", "v1.7.0"],
    "web-frontend": ["v4.2.0", "v4.1.1", "v4.0.0"],
}
_ENGINEERS = ["alice", "bob", "chen", "diaz", "erin"]
_STATUSES = ["success", "success", "success", "rolled_back", "in_progress"]


def seed_rows() -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    base = dt.datetime(2026, 11, 25, 9, 0)
    rid = 1
    # 21 days x 4 services = 84 records, spread across versions/statuses.
    for day_offset in range(20, -1, -1):
        day = (base - dt.timedelta(days=day_offset)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        for si, service in enumerate(_SERVICES):
            versions = _VERSIONS[service]
            version = versions[(rid + si) % len(versions)]
            env = "prod" if rid % 3 != 0 else "staging"
            status = _STATUSES[rid % len(_STATUSES)]
            when = day + dt.timedelta(hours=8 + (rid % 10), minutes=(rid * 7) % 60)
            by = _ENGINEERS[rid % len(_ENGINEERS)]
            rows.append(
                (rid, service, version, env, status, when.replace(tzinfo=None), by)
            )
            rid += 1
    return rows


def seed_if_empty(conn) -> int:
    """Create table and seed rows. Returns number of rows inserted."""
    conn.execute(SEED_SQL)
    count = conn.execute("SELECT count(*) FROM deployments").fetchone()[0]
    if count:
        return 0
    rows = seed_rows()
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO deployments "
            "(id, service, version, environment, status, deployed_at, deployed_by) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s)",
            rows,
        )
    return len(rows)
