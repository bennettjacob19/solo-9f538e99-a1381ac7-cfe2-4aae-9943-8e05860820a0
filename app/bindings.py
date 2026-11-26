"""Per-session named query bindings.

Names are scoped by session: the same query name in two sessions identifies
two independent bindings. The store is thread-safe; batches receive an
immutable snapshot at acceptance time.
"""
from __future__ import annotations

import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any

from .sqlnorm import CanonicalSql, SqlValidationError, normalize_sql
from .templates import TEMPLATES


class BindingError(KeyError):
    code = "binding_error"


class BindingNotFound(BindingError):
    code = "binding_not_found"


class BindingValidationError(ValueError):
    code = "binding_validation_error"


@dataclass(frozen=True)
class Binding:
    session: str
    name: str
    sql: str                       # canonical $n SQL
    key: str
    statement_name: str
    param_names: tuple[str, ...]
    template: str | None
    title: str | None
    description: str | None
    version: int
    created_at: float
    updated_at: float
    rebind_count: int

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["param_names"] = list(self.param_names)
        return d


@dataclass
class _Versioned:
    binding: Binding


class BindingStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        # session -> name -> Binding
        self._sessions: dict[str, dict[str, Binding]] = {}

    # ---- writes -------------------------------------------------------
    def put(
        self,
        session: str,
        name: str,
        *,
        sql: str | None = None,
        template: str | None = None,
        title: str | None = None,
        description: str | None = None,
    ) -> tuple[Binding, Binding | None]:
        """Create or rebind. Returns (new_binding, old_binding_or_None)."""
        session = _clean(session, "session")
        name = _clean(name, "name")

        canonical: CanonicalSql
        tpl_id: str | None = None
        tpl_title: str | None = None
        tpl_desc: str | None = None
        template_param_names: tuple[str, ...] | None = None
        sql_explicit = sql is not None and bool(str(sql).strip())

        if template:
            if template not in TEMPLATES:
                raise BindingValidationError(
                    f"未知模板 {template!r}；可用模板: {sorted(TEMPLATES)}"
                )
            tpl = TEMPLATES[template]
            tpl_id = template
            tpl_title = tpl["title"]
            tpl_desc = tpl["description"]
            template_param_names = tuple(tpl.get("param_names", ()))
            # Template provides the default SQL; an explicit sql may override.
            if sql is None:
                sql = tpl["sql"]

        if sql is None or not str(sql).strip():
            raise BindingValidationError(
                "必须提供 sql，或通过 template 选择内置参数化 SQL。"
            )

        try:
            canonical = normalize_sql(sql)
        except SqlValidationError as exc:
            raise BindingValidationError(str(exc)) from exc

        # Template SQL carries meaningful parameter names; otherwise native
        # $n placeholders get synthetic p1..pN names (lists still work).
        param_names = canonical.param_names
        if (
            not sql_explicit
            and template_param_names
            and len(template_param_names) == canonical.param_count
        ):
            param_names = template_param_names

        now = time.time()
        with self._lock:
            bucket = self._sessions.setdefault(session, {})
            old = bucket.get(name)
            binding = Binding(
                session=session,
                name=name,
                sql=canonical.text,
                key=canonical.key,
                statement_name=canonical.statement_name,
                param_names=param_names,
                template=tpl_id or old.template if old else tpl_id,
                title=title or tpl_title or (old.title if old else None),
                description=description
                or tpl_desc
                or (old.description if old else None),
                version=(old.version + 1) if old else 1,
                created_at=old.created_at if old else now,
                updated_at=now,
                rebind_count=(old.rebind_count + 1) if old else 0,
            )
            bucket[name] = binding
            return binding, old

    def delete(self, session: str, name: str) -> Binding:
        session = _clean(session, "session")
        name = _clean(name, "name")
        with self._lock:
            bucket = self._sessions.get(session)
            if not bucket or name not in bucket:
                raise BindingNotFound(f"会话 {session!r} 中没有查询名 {name!r}。")
            return bucket.pop(name)

    # ---- reads --------------------------------------------------------
    def get(self, session: str, name: str) -> Binding:
        with self._lock:
            bucket = self._sessions.get(session)
            if not bucket or name not in bucket:
                known = sorted(bucket) if bucket else []
                raise BindingNotFound(
                    f"会话 {session!r} 中没有查询名 {name!r}。"
                    + (f" 该会话已有: {known}" if known else " 该会话还没有任何绑定。")
                )
            return bucket[name]

    def get_many(self, refs: list[tuple[str, str]]) -> list[Binding]:
        """Resolve a whole batch against one consistent binding snapshot."""
        with self._lock:
            return [self.get(session, name) for session, name in refs]

    def find_many(self, refs: list[tuple[str, str]]) -> list["Binding | None"]:
        """Like :meth:`get_many` but preserves positions with None for missing."""
        with self._lock:
            out: list[Binding | None] = []
            for session, name in refs:
                bucket = self._sessions.get(session)
                out.append(bucket.get(name) if bucket else None)
            return out

    def list_session(self, session: str) -> list[Binding]:
        with self._lock:
            return list(self._sessions.get(_clean(session, "session"), {}).values())

    def list_all(self) -> dict[str, list[Binding]]:
        with self._lock:
            return {s: list(names.values()) for s, names in sorted(self._sessions.items())}

    def sessions(self) -> list[dict[str, Any]]:
        with self._lock:
            out = []
            for session, names in sorted(self._sessions.items()):
                out.append(
                    {
                        "session": session,
                        "query_names": sorted(names),
                        "binding_count": len(names),
                    }
                )
            return out

    def resolve_snapshot_item(self, session: str, name: str) -> Binding:
        return self.get(session, name)


def _clean(value: Any, field_name: str) -> str:
    if value is None:
        raise BindingValidationError(f"{field_name} 不能为空。")
    text = str(value).strip()
    if not text:
        raise BindingValidationError(f"{field_name} 不能为空字符串。")
    if len(text) > 128:
        raise BindingValidationError(f"{field_name} 最长 128 字符。")
    return text
