"""Built-in parameterized SQL templates over the deployment records table."""
from __future__ import annotations

from typing import Any

TEMPLATES: dict[str, dict[str, Any]] = {
    "deployments_by_service": {
        "id": "deployments_by_service",
        "title": "按服务筛选部署记录",
        "description": "传入服务名（精确匹配），返回该服务的部署记录，按部署时间倒序。",
        "filters": ["service"],
        "param_names": ["service"],
        "examples": [{"service": "api-gateway"}],
        "sql": (
            "SELECT id, service, version, environment, status, deployed_at, deployed_by "
            "FROM deployments WHERE service = $1 ORDER BY deployed_at DESC, id DESC"
        ),
    },
    "deployments_by_version": {
        "id": "deployments_by_version",
        "title": "按版本筛选部署记录",
        "description": "传入版本号（精确匹配），返回部署了该版本的所有服务记录。",
        "filters": ["version"],
        "param_names": ["version"],
        "examples": [{"version": "v2.0.1"}],
        "sql": (
            "SELECT id, service, version, environment, status, deployed_at, deployed_by "
            "FROM deployments WHERE version = $1 ORDER BY deployed_at DESC, id DESC"
        ),
    },
    "deployments_by_date": {
        "id": "deployments_by_date",
        "title": "按日期筛选部署记录",
        "description": "传入日期（YYYY-MM-DD），返回当天（按 deployed_at 的日期部分）的部署记录。",
        "filters": ["date"],
        "param_names": ["date"],
        "examples": [{"date": "2026-11-10"}],
        "sql": (
            "SELECT id, service, version, environment, status, deployed_at, deployed_by "
            "FROM deployments WHERE deployed_at::date = $1 "
            "ORDER BY deployed_at DESC, id DESC"
        ),
    },
    "deployments_by_service_and_version": {
        "id": "deployments_by_service_and_version",
        "title": "按服务 + 版本筛选",
        "description": "同时传入服务名与版本号，精确筛选。",
        "filters": ["service", "version"],
        "param_names": ["service", "version"],
        "examples": [{"service": "report-worker", "version": "v1.8.3"}],
        "sql": (
            "SELECT id, service, version, environment, status, deployed_at, deployed_by "
            "FROM deployments WHERE service = $1 AND version = $2 "
            "ORDER BY deployed_at DESC, id DESC"
        ),
    },
    "deployments_between_dates": {
        "id": "deployments_between_dates",
        "title": "按日期区间筛选",
        "description": "传入起止日期（含两端，YYYY-MM-DD），返回区间内的部署记录。",
        "filters": ["date"],
        "param_names": ["date_from", "date_to"],
        "examples": [{"date_from": "2026-11-01", "date_to": "2026-11-15"}],
        "sql": (
            "SELECT id, service, version, environment, status, deployed_at, deployed_by "
            "FROM deployments "
            "WHERE deployed_at::date BETWEEN $1 AND $2 "
            "ORDER BY deployed_at DESC, id DESC"
        ),
    },
    "deployments_filter": {
        "id": "deployments_filter",
        "title": "服务 / 版本 / 日期组合筛选（参数可空）",
        "description": (
            "三个参数分别为 service、version、date；传 null 表示不按该条件筛选。"
        ),
        "filters": ["service", "version", "date"],
        "param_names": ["service", "version", "date"],
        "examples": [{"service": "api-gateway", "version": None, "date": None}],
        "sql": (
            "SELECT id, service, version, environment, status, deployed_at, deployed_by "
            "FROM deployments "
            "WHERE ($1::text IS NULL OR service = $1) "
            "AND ($2::text IS NULL OR version = $2) "
            "AND ($3::date IS NULL OR deployed_at::date = $3) "
            "ORDER BY deployed_at DESC, id DESC"
        ),
    },
}


def list_templates() -> list[dict]:
    return [
        {k: v for k, v in tpl.items()}
        for tpl in TEMPLATES.values()
    ]
