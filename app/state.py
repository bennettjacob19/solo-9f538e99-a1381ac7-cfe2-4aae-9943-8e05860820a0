"""Process-wide singletons. DATABASE_URL is read from the environment at import
time, so tests must set it before importing :mod:`app.main`."""
from __future__ import annotations

from .bindings import BindingStore
from .config import get_settings
from .runtime import BatchRuntime

settings = get_settings()
bindings = BindingStore()
runtime = BatchRuntime(
    settings.database_url,
    connection_count=settings.connection_count,
    cap=settings.prepared_per_connection,
)
