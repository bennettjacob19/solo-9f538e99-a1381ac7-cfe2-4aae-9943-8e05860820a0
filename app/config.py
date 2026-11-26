"""Runtime settings (environment overrides)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field

from .devdb import ensure_local_database_url


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    database_url: str = field(default_factory=ensure_local_database_url)
    # Scenario constants: only two PG connections and two real prepared
    # statements per connection. Exposed as settings only for tests.
    connection_count: int = 2
    prepared_per_connection: int = 2
    max_batch_queries: int = 8
    # How long startup waits for PostgreSQL (docker compose first boot).
    db_connect_timeout_seconds: float = float(os.getenv("DB_CONNECT_TIMEOUT", "60"))
    seed_deployments: bool = os.getenv("SEED_DEPLOYMENTS", "1") not in ("0", "false", "False")


def get_settings() -> Settings:
    return Settings()
