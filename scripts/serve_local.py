#!/usr/bin/env python3
"""Run the whole drill locally without Docker.

It boots a real PostgreSQL using the pgserver wheel, points DATABASE_URL at it,
then starts uvicorn. Requires the dev extras::

    pip install -r requirements-dev.txt
    python scripts/serve_local.py

Then open http://127.0.0.1:8000/docs and run:

    python scripts/demo_http.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def main() -> None:
    import pgserver
    import uvicorn

    data_dir = os.environ.get("PGDATA_LOCAL", "/tmp/pg_two_connection_drill")
    server = pgserver.get_server(data_dir)
    dsn = server.get_uri()
    os.environ["DATABASE_URL"] = dsn
    os.environ.setdefault("DB_CONNECT_TIMEOUT", "30")

    host = os.environ.get("HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", "8000"))
    print(f"[serve_local] PostgreSQL DSN: {dsn}")
    print(f"[serve_local] API + Swagger: http://{host}:{port}/docs")
    uvicorn.run("app.main:app", host=host, port=port, log_level="warning")


if __name__ == "__main__":
    main()
