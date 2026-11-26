"""HTTP API routes (included both at root and under /api)."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from . import state
from .bindings import Binding, BindingNotFound, BindingValidationError
from .models import BatchIn, BindingUpsert
from .runtime import BatchBusy, BatchExecutionError, ResolvedQuery
from .sqlnorm import map_params
from .templates import TEMPLATES, list_templates

router = APIRouter(tags=["drill"])


# ---------------------------------------------------------------------------
# Templates
# ---------------------------------------------------------------------------
@router.get("/templates", summary="内置参数化 SQL 模板（按服务/版本/日期筛选）")
def get_templates() -> dict[str, Any]:
    return {"count": len(TEMPLATES), "templates": list_templates()}


# ---------------------------------------------------------------------------
# Session-scoped bindings
# ---------------------------------------------------------------------------
def _binding_payload(b: Binding) -> dict[str, Any]:
    d = b.to_dict()
    d["links"] = {
        "self": f"/api/sessions/{b.session}/bindings/{b.name}",
        "run_example": "/api/batches",
    }
    return d


def _upsert(session: str | None, name: str | None, payload: BindingUpsert) -> dict[str, Any]:
    session = session or payload.session
    name = name or payload.name
    if not session or not name:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "missing_session_or_name",
                "message": "必须在路径或请求体中提供 session 与查询名 name。",
            },
        )
    try:
        binding, old = state.bindings.put(
            session,
            name,
            sql=payload.sql,
            template=payload.template,
            title=payload.title,
            description=payload.description,
        )
    except BindingValidationError as exc:
        raise HTTPException(
            status_code=422, detail={"code": "binding_validation_error", "message": str(exc)}
        ) from exc

    return {
        "binding": _binding_payload(binding),
        "rebound": old is not None,
        "previous_version": old.version if old else None,
        "previous_sql": old.sql if old else None,
        "message": (
            f"已改绑 {session}/{name}（v{old.version} -> v{binding.version}），"
            "历史批次仍沿用受理时的旧绑定快照。"
            if old
            else f"已在会话 {session!r} 下新增查询名 {name!r}（v1）。"
        ),
    }


@router.get("/sessions", summary="列出所有会话及其查询名")
def list_sessions() -> dict[str, Any]:
    return {"sessions": state.bindings.sessions()}


@router.get("/sessions/{session}/bindings", summary="列出某会话的全部绑定")
def list_bindings(session: str) -> dict[str, Any]:
    items = state.bindings.list_session(session)
    return {
        "session": session,
        "count": len(items),
        "bindings": [_binding_payload(b) for b in items],
    }


@router.post("/sessions/{session}/bindings", summary="在指定会话下新增/改绑（名字在 body）")
def create_session_binding(session: str, payload: BindingUpsert) -> dict[str, Any]:
    return _upsert(session, payload.name, payload)


@router.put(
    "/sessions/{session}/bindings/{name}",
    summary="新增或改绑某会话下的查询名（/docs 里直接 Try it out）",
)
def put_session_binding(session: str, name: str, payload: BindingUpsert) -> dict[str, Any]:
    return _upsert(session, name, payload)


@router.get("/sessions/{session}/bindings/{name}", summary="查看单个绑定")
def get_session_binding(session: str, name: str) -> dict[str, Any]:
    try:
        return _binding_payload(state.bindings.get(session, name))
    except BindingNotFound as exc:
        raise HTTPException(
            status_code=404, detail={"code": "binding_not_found", "message": str(exc)}
        ) from exc


@router.delete("/sessions/{session}/bindings/{name}", summary="删除一个绑定")
def delete_session_binding(session: str, name: str) -> dict[str, Any]:
    try:
        removed = state.bindings.delete(session, name)
    except BindingNotFound as exc:
        raise HTTPException(
            status_code=404, detail={"code": "binding_not_found", "message": str(exc)}
        ) from exc
    return {"deleted": True, "binding": _binding_payload(removed)}


# Root-level generic binding aliases (PUT/POST /bindings, GET /bindings)
@router.get("/bindings", summary="全部会话的绑定（通用入口）")
def all_bindings() -> dict[str, Any]:
    data = state.bindings.list_all()
    return {
        "sessions": [
            {"session": s, "count": len(bs), "bindings": [_binding_payload(b) for b in bs]}
            for s, bs in data.items()
        ]
    }


@router.post("/bindings", summary="新增/改绑（body 带 session 与 name）")
def post_binding(payload: BindingUpsert) -> dict[str, Any]:
    return _upsert(payload.session, payload.name, payload)


@router.put("/bindings", summary="新增/改绑（PUT 通用入口）")
def put_binding(payload: BindingUpsert) -> dict[str, Any]:
    return _upsert(payload.session, payload.name, payload)


# ---------------------------------------------------------------------------
# Connections / prepared-statement cache
# ---------------------------------------------------------------------------
@router.get("/connections", summary="两条 PostgreSQL 连接及后端 PID")
def get_connections() -> dict[str, Any]:
    conns = state.runtime.connections_state()
    return {"connection_count": len(conns), "max_connections": 2, "connections": conns}


@router.get("/cache", summary="每条连接上的真实预备语句缓存（LRU，至多 2 条）")
def get_cache() -> dict[str, Any]:
    conns = state.runtime.cache_state()
    return {
        "prepared_per_connection_max": state.settings.prepared_per_connection,
        "connections": conns,
    }


@router.post("/cache/reconcile", summary="以 pg_prepared_statements 为准核对缓存")
def reconcile_cache() -> dict[str, Any]:
    conns = state.runtime.reconcile_all()
    return {"reconciled": True, "connections": conns}


# ---------------------------------------------------------------------------
# Deployment records (built-in seed data)
# ---------------------------------------------------------------------------
@router.get("/deployments", summary="查询内置部署记录（可按服务/版本/日期筛选）")
def list_deployments(
    service: str | None = Query(default=None),
    version: str | None = Query(default=None),
    date: str | None = Query(default=None, description="YYYY-MM-DD"),
    environment: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    where: list[str] = []
    params: list[Any] = []
    if service:
        where.append("service = %s")
        params.append(service)
    if version:
        where.append("version = %s")
        params.append(version)
    if date:
        where.append("deployed_at::date = %s")
        params.append(date)
    if environment:
        where.append("environment = %s")
        params.append(environment)

    sql = (
        "SELECT id, service, version, environment, status, deployed_at, deployed_by "
        "FROM deployments"
    )
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY deployed_at DESC, id DESC LIMIT %s"
    params.append(limit)

    slot = state.runtime.slots[0]
    with slot.lock:
        slot.ensure_conn_locked()
        assert slot.conn is not None
        cur = slot.conn.execute(sql, params)
        columns = [d.name for d in cur.description]
        rows = cur.fetchall()

    from .runtime import json_safe

    safe_rows = [[json_safe(v) for v in row] for row in rows]
    return {
        "count": len(safe_rows),
        "columns": columns,
        "records": [dict(zip(columns, row)) for row in safe_rows],
    }


# ---------------------------------------------------------------------------
# Batches
# ---------------------------------------------------------------------------
def resolve_queries(payload: BatchIn) -> list[ResolvedQuery]:
    # One atomic lookup gives the whole batch a consistent acceptance snapshot.
    refs = [(q.session, q.name) for q in payload.queries]
    bindings = state.bindings.find_many(refs)

    resolved: list[ResolvedQuery] = []
    for i, q in enumerate(payload.queries):
        binding = bindings[i]
        if binding is None:
            known = [b.name for b in state.bindings.list_session(q.session)]
            raise HTTPException(
                status_code=404,
                detail={
                    "code": "binding_not_found",
                    "message": (
                        f"会话 {q.session!r} 中没有查询名 {q.name!r}。"
                        + (f" 该会话已有: {sorted(known)}" if known else "")
                    ),
                    "query_index": i,
                    "session": q.session,
                    "query_name": q.name,
                },
            )

        try:
            positional = map_params(q.params, binding.param_names)
        except ValueError as exc:
            raise HTTPException(
                status_code=422,
                detail={
                    "code": "invalid_params",
                    "message": str(exc),
                    "query_index": i,
                    "session": q.session,
                    "query_name": q.name,
                    "param_names": list(binding.param_names),
                },
            ) from exc

        inclusive = (
            q.inclusive_deadline
            if q.inclusive_deadline is not None
            else payload.inclusive_deadline
        )
        resolved.append(
            ResolvedQuery(
                index=i,
                session=q.session,
                name=q.name,
                binding_version=binding.version,
                template=binding.template,
                sql=binding.sql,
                key=binding.key,
                statement_name=binding.statement_name,
                param_names=list(binding.param_names),
                positional_params=positional,
                arrival=q.arrival_tick,
                duration=q.duration_ticks,
                deadline=q.deadline_tick,
                deadline_inclusive=inclusive,
            )
        )
    return resolved


def _run_batch(payload: BatchIn) -> dict[str, Any]:
    resolved = resolve_queries(payload)
    try:
        return state.runtime.submit_batch(
            resolved=resolved,
            max_prepares=payload.max_prepares,
            inclusive_deadline=payload.inclusive_deadline,
            dry_run=payload.dry_run,
            request_echo={"query_count": len(payload.queries)},
        )
    except BatchBusy as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "batch_busy",
                "message": str(exc),
                "active_batch_id": state.runtime.active_batch_id,
            },
        ) from exc
    except BatchExecutionError as exc:
        # The record is the authoritative partial/error batch report.
        return exc.record


@router.post("/batches/plan", summary="只排程试算：是否受理、最少 PREPARE、起止拍")
def plan_batch_only(payload: BatchIn) -> dict[str, Any]:
    payload.dry_run = True
    record = _run_batch(payload)
    # Planning endpoint always answers 200; inspect status/accepted.
    return record


@router.post("/batches", summary="提交一批（至多 8 个查询）：排程并在 PostgreSQL 实际取数")
def submit_batch(payload: BatchIn):
    record = _run_batch(payload)
    if record.get("status") == "rejected":
        return JSONResponse(status_code=422, content=record)
    if record.get("status") == "error":
        return JSONResponse(status_code=500, content=record)
    return record


@router.post("/batches/submit", summary="信封风格：始终 HTTP 200，用 status 判成败")
def submit_batch_envelope(payload: BatchIn) -> dict[str, Any]:
    try:
        record = _run_batch(payload)
    except HTTPException as exc:
        return {
            "ok": False,
            "http_status": exc.status_code,
            "status": "invalid_request",
            "error": exc.detail,
        }
    return {
        "ok": record.get("status") == "completed"
        or record.get("status") == "preview_feasible",
        "http_status": 200,
        "status": record.get("status"),
        "result": record,
    }


@router.get("/batches", summary="批次列表（摘要）")
def list_batches(limit: int = Query(default=50, ge=1, le=200)) -> dict[str, Any]:
    return {"batches": state.runtime.store.list(limit=limit)}


@router.get("/batches/{batch_id}", summary="批次详情（排程、PID、结果、错误）")
def get_batch(batch_id: str) -> dict[str, Any]:
    record = state.runtime.store.get(batch_id)
    if record is None:
        raise HTTPException(
            status_code=404, detail={"code": "batch_not_found", "batch_id": batch_id}
        )
    return record
