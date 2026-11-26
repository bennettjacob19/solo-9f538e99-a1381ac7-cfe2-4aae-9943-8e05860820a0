"""Request schemas. Input fields accept several common naming variants so
different clients/demos can use their preferred vocabulary."""
from __future__ import annotations

from typing import Any

from pydantic import AliasChoices, BaseModel, ConfigDict, Field


class QueryIn(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    session: str = Field(
        ...,
        min_length=1,
        max_length=128,
        validation_alias=AliasChoices("session", "session_id", "sid"),
        description="会话 ID。查询名按会话隔离。",
    )
    name: str = Field(
        ...,
        min_length=1,
        max_length=128,
        validation_alias=AliasChoices("name", "query_name", "qname"),
        description="该会话下的查询名（绑定名）。",
    )
    params: Any = Field(
        default=None,
        validation_alias=AliasChoices("params", "parameters", "args", "bind_params"),
        description="位置参数数组、命名参数对象，单参数 SQL 也可直接给标量。",
    )
    arrival_tick: int = Field(
        default=0,
        ge=0,
        validation_alias=AliasChoices(
            "arrival_tick", "arrival", "arrival_ticks", "arrival_beat",
            "release_tick", "release", "arrive_tick",
        ),
        description="到达拍（最早可开始的拍）。",
    )
    duration_ticks: int = Field(
        ...,
        ge=1,
        validation_alias=AliasChoices(
            "duration_ticks", "duration", "occupy_ticks", "occupy_beats",
            "ticks", "work_ticks", "processing_ticks", "cost_ticks", "busy_ticks",
        ),
        description="独占连接的占用拍数。",
    )
    deadline_tick: int = Field(
        ...,
        validation_alias=AliasChoices(
            "deadline_tick", "deadline", "deadline_ticks", "latest_finish_tick",
            "latest_complete_tick", "must_finish_by", "ddl",
        ),
        description="最迟完成拍；默认约定 start+duration <= deadline。",
    )
    inclusive_deadline: bool | None = Field(
        default=None,
        validation_alias=AliasChoices("inclusive_deadline", "deadline_inclusive"),
        description="覆盖整批的截止拍口径：true 时要求最后占用拍 <= deadline。",
    )


class BatchIn(BaseModel):
    model_config = ConfigDict(
        extra="ignore",
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {
                    "summary": "限额内可受理的一批",
                    "value": {
                        "max_prepares": 2,
                        "queries": [
                            {
                                "session": "alice",
                                "name": "deploy_by_service",
                                "params": {"service": "api-gateway"},
                                "arrival_tick": 0,
                                "duration_ticks": 2,
                                "deadline_tick": 2,
                            },
                            {
                                "session": "bob",
                                "name": "deploy_by_version",
                                "params": ["v2.0.1"],
                                "arrival_tick": 0,
                                "duration_ticks": 1,
                                "deadline_tick": 3,
                            },
                        ],
                    },
                }
            ]
        },
    )

    queries: list[QueryIn] = Field(
        ...,
        min_length=1,
        max_length=8,
        description="一批 1..8 个查询。",
    )
    max_prepares: int = Field(
        ...,
        ge=0,
        validation_alias=AliasChoices(
            "max_prepares", "prepare_limit", "max_prepare_count",
            "prepare_budget", "max_prepare_times", "prepare_quota",
        ),
        description="全批真实 PREPARE 次数上限。",
    )
    inclusive_deadline: bool = Field(
        default=False,
        validation_alias=AliasChoices("inclusive_deadline", "deadline_inclusive"),
        description="true 时截止拍按包含端点解释。",
    )
    dry_run: bool = Field(
        default=False,
        validation_alias=AliasChoices("dry_run", "plan_only", "preview"),
        description="只排程试算，不执行、不改动连接缓存。",
    )


class BindingUpsert(BaseModel):
    model_config = ConfigDict(
        extra="ignore",
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {
                    "summary": "选用内置『按服务筛选』模板",
                    "value": {
                        "template": "deployments_by_service",
                        "description": "值班看板：按服务查部署",
                    },
                },
                {
                    "summary": "改绑为自定义命名参数 SQL",
                    "value": {
                        "sql": (
                            "SELECT count(*) FROM deployments "
                            "WHERE service = :service AND status = :status"
                        )
                    },
                },
            ]
        },
    )

    session: str | None = Field(
        default=None,
        max_length=128,
        validation_alias=AliasChoices("session", "session_id"),
    )
    name: str | None = Field(
        default=None,
        max_length=128,
        validation_alias=AliasChoices("name", "query_name", "qname"),
    )
    template: str | None = Field(
        default=None,
        validation_alias=AliasChoices("template", "template_id", "from_template"),
        description="内置模板 ID，见 GET /api/templates。",
    )
    sql: str | None = Field(
        default=None,
        description=(
            "参数化 SQL，支持 $1 位置参数、:name 或 %(name)s 命名参数。"
            "给出时优先于 template 自带 SQL。"
        ),
    )
    title: str | None = None
    description: str | None = None
