"""Normalize parameterized SQL.

Bindings may be written with any of:

* native PostgreSQL positional placeholders: ``$1``, ``$2`` ...
* psycopg/python named placeholders: ``%(service)s``
* ``:name`` placeholders (but never confuse PostgreSQL ``::type`` casts)

They are all converted to canonical ``$n`` SQL so the same SQL text always
maps to the same physical prepared statement name.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

_NAMED_PERCENT_RE = re.compile(r"%\(([A-Za-z_][A-Za-z0-9_]*)\)s")
_COLON_RE = re.compile(r":([A-Za-z_][A-Za-z0-9_]*)")
_DOLLAR_RE = re.compile(r"\$([0-9]+)")
_PLACEHOLDER_RE = re.compile(
    r"%\((?P<pname>[A-Za-z_][A-Za-z0-9_]*)\)s"
    r"|:(?P<cname>[A-Za-z_][A-Za-z0-9_]*)"
    r"|\$(?P<dnum>[0-9]+)"
)


class SqlValidationError(ValueError):
    pass


@dataclass(frozen=True)
class CanonicalSql:
    text: str
    param_names: tuple[str, ...]

    @property
    def param_count(self) -> int:
        return len(self.param_names)

    @property
    def key(self) -> str:
        return sql_key(self.text)

    @property
    def statement_name(self) -> str:
        return statement_name_for_key(self.key)


def sql_key(canonical_text: str) -> str:
    """Short stable identity for a SQL text (cache key / statement name)."""
    digest = hashlib.sha1(canonical_text.encode("utf-8")).hexdigest()
    return digest[:16]


def statement_name_for_key(key: str) -> str:
    # PostgreSQL identifiers may be up to 63 bytes; prefix + 16 is tiny.
    return f"rq_{key}"


def _scan_segments(sql: str):
    """Yield (kind, start, end) for code segments vs quoted/comment blocks.

    kind is 'code' or 'skip'. Placeholder rewriting only happens in code.
    """
    n = len(sql)
    i = 0
    code_start = 0
    segments: list[tuple[str, int, int]] = []

    def flush_code(end: int) -> None:
        nonlocal code_start
        if end > code_start:
            segments.append(("code", code_start, end))

    while i < n:
        ch = sql[i]
        # single quoted string, '' is an escaped quote
        if ch == "'":
            flush_code(i)
            j = i + 1
            while j < n:
                if sql[j] == "'":
                    if j + 1 < n and sql[j + 1] == "'":
                        j += 2
                        continue
                    j += 1
                    break
                j += 1
            segments.append(("skip", i, j))
            i = j
            code_start = j
            continue
        # double quoted identifier
        if ch == '"':
            flush_code(i)
            j = i + 1
            while j < n:
                if sql[j] == '"':
                    if j + 1 < n and sql[j + 1] == '"':
                        j += 2
                        continue
                    j += 1
                    break
                j += 1
            segments.append(("skip", i, j))
            i = j
            code_start = j
            continue
        # line comment
        if ch == "-" and i + 1 < n and sql[i + 1] == "-":
            flush_code(i)
            j = sql.find("\n", i + 2)
            j = n if j == -1 else j + 1
            segments.append(("skip", i, j))
            i = j
            code_start = j
            continue
        # block comment (PostgreSQL allows nesting)
        if ch == "/" and i + 1 < n and sql[i + 1] == "*":
            flush_code(i)
            depth = 1
            j = i + 2
            while j + 1 < n and depth > 0:
                if sql[j] == "/" and sql[j + 1] == "*":
                    depth += 1
                    j += 2
                elif sql[j] == "*" and sql[j + 1] == "/":
                    depth -= 1
                    j += 2
                else:
                    j += 1
            if depth == 0:
                j = max(j, i + 2)
            else:
                j = n
            segments.append(("skip", i, j))
            i = j
            code_start = j
            continue
        i += 1

    flush_code(n)
    return segments


def assert_single_statement(sql: str) -> None:
    """Reject embedded extra statements (semicolon followed by real SQL)."""
    for kind, start, end in _scan_segments(sql):
        if kind != "code":
            continue
        segment = sql[start:end]
        pos = 0
        while True:
            semi = segment.find(";", pos)
            if semi == -1:
                break
            tail = segment[semi + 1 :].strip()
            if tail:
                raise SqlValidationError(
                    "只允许单条 SQL 语句；检测到分号后还有额外内容。"
                )
            pos = semi + 1


def normalize_sql(raw: str) -> CanonicalSql:
    """Convert supported placeholder styles to canonical ``$n`` SQL."""
    if not raw or not raw.strip():
        raise SqlValidationError("SQL 不能为空。")

    assert_single_statement(raw)
    sql = raw.strip().rstrip(";").strip()

    pieces: list[str] = []
    names: list[str] = []
    name_to_pos: dict[str, int] = {}
    used_dollar: list[int] = []
    used_named = False

    def pos_for(name: str) -> int:
        nonlocal used_named
        used_named = True
        if name not in name_to_pos:
            name_to_pos[name] = len(names) + 1
            names.append(name)
        return name_to_pos[name]

    for kind, start, end in _scan_segments(sql):
        text = sql[start:end]
        if kind == "skip":
            pieces.append(text)
            continue

        def repl(m: re.Match) -> str:
            pname = m.group("pname")
            if pname is not None:
                return f"${pos_for(pname)}"
            cname = m.group("cname")
            if cname is not None:
                # Never treat the second colon of a ::cast as a named param.
                if m.start() > 0 and text[m.start() - 1] == ":":
                    return m.group(0)
                return f"${pos_for(cname)}"
            dnum = m.group("dnum")
            used_dollar.append(int(dnum))
            return m.group(0)

        pieces.append(_PLACEHOLDER_RE.sub(repl, text))

    canonical = "".join(pieces)

    if used_named and used_dollar:
        raise SqlValidationError(
            "不能在同一条 SQL 中混用命名参数(:name / %(name)s)与 $n 位置参数。"
        )

    if not used_named and used_dollar:
        max_pos = max(used_dollar)
        missing = [i for i in range(1, max_pos + 1) if i not in used_dollar]
        if missing:
            raise SqlValidationError(
                f"位置参数编号不连续，缺少 ${', $'.join(map(str, missing))}。"
            )
        names = [f"p{i}" for i in range(1, max_pos + 1)]

    return CanonicalSql(text=canonical, param_names=tuple(names))


def map_params(params: object, param_names: tuple[str, ...]) -> list[object]:
    """Map batch-provided params (list/dict/scalar) to positional values."""
    count = len(param_names)

    if count == 0:
        if params in (None, [], {}):
            return []
        # Extra params for parameterless SQL: reject to avoid silent mistakes.
        raise ValueError("该 SQL 没有参数，但请求里提供了参数。")

    if params is None:
        if count == 1:
            return [None]
        raise ValueError(f"该 SQL 需要 {count} 个参数，但未提供 params。")

    if isinstance(params, dict):
        result: list[object] = []
        for name in param_names:
            if name in params:
                result.append(params[name])
            # Named binding produced by :service keeps its real name;
            # positional $n produced synthetic p1..pN which a dict caller
            # cannot reasonably know, so also accept str(index).
            elif name.startswith("p") and name[1:].isdigit():
                idx = int(name[1:]) - 1
                alt = str(idx + 1)
                if alt in params:
                    result.append(params[alt])
                else:
                    raise ValueError(f"缺少参数: {name}")
            else:
                raise ValueError(f"缺少参数: {name}")
        return result

    if isinstance(params, (list, tuple)):
        if len(params) != count:
            raise ValueError(f"参数个数不符：SQL 需要 {count} 个，实际给了 {len(params)} 个。")
        return list(params)

    # Scalar shorthand for single-parameter SQL.
    if count == 1:
        return [params]

    raise ValueError(
        f"参数必须是列表或对象；单参数 SQL 也允许直接给标量。SQL 需要 {count} 个参数。"
    )
