"""Dev-only database auto-provisioning.

Resolution order for the DSN:

1. ``DATABASE_URL`` is set  -> use it (this is what docker compose does).
2. The default local DSN is reachable -> use it.
3. Otherwise, if the ``pgserver`` package is installed (requirements-dev),
   boot a user-space real PostgreSQL and return its DSN.

In the Docker image ``DATABASE_URL`` is always provided and ``pgserver`` is not
installed, so this fallback is inert there.
"""
from __future__ import annotations

import os

DEFAULT_DATABASE_URL = "postgresql://drill:drill@localhost:5432/drill"

_embedded_server = None


def ensure_local_database_url() -> str:
    explicit = os.getenv("DATABASE_URL")
    if explicit:
        return explicit

    try:
        import psycopg

        with psycopg.connect(DEFAULT_DATABASE_URL, connect_timeout=2) as conn:
            conn.execute("SELECT 1")
        return DEFAULT_DATABASE_URL
    except Exception:
        pass

    try:
        import pgserver
    except ImportError:
        return DEFAULT_DATABASE_URL

    global _embedded_server
    data_dir = os.environ.get("PGDATA_LOCAL", "/tmp/pg_two_connection_drill")
    _embedded_server = pgserver.get_server(data_dir)
    dsn = _embedded_server.get_uri()
    print(f"[devdb] DATABASE_URL not set; started embedded PostgreSQL: {dsn}")
    return dsn
