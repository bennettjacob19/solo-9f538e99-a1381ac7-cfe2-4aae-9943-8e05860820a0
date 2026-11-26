# 报表高峰 · 双 PostgreSQL 连接取数演练（FastAPI + psycopg 3）

发布报表高峰只有 **两条 PostgreSQL 连接** 可用；每条连接最多保留 **两条真实
`PREPARE` 语句**，命中就复用、跨批保留。等一条已有预备语句的连接可以省一次
`PREPARE`，却可能排队错过最迟完成拍。本项目在本地把「绑定管理 → 精确排程 →
限额/时限判定 → 真实 PostgreSQL 取数 → 出错核对缓存」完整跑通，并提供
`docker compose` 一键启动与 Swagger `/docs`。

---

## 1. 快速开始

### 方式 A：Docker Compose（推荐）

```bash
docker compose up --build
# 打开 http://localhost:8000/docs
```

- `db`：`postgres:16-alpine`，带 `pg_isready` 健康检查与命名卷 `pgdata`。
- `app`：FastAPI/uvicorn，启动时重试连接数据库并自动建表、写入 **84 条内置
  部署记录**（`deployments` 表，覆盖 4 个服务 / 4 个版本线 / 21 天）。
- 应用始终只创建 **2 条长连接**。
- 宿主机 Postgres 端口映射到 **55432**（避免和本机 5432 冲突）；应用端口可用
  `APP_PORT=8010 docker compose up` 覆盖。

### 方式 B：纯本地（无需 Docker，开发/自测）

需要 Python 3.11+。

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements-dev.txt   # 含 pgserver：用户态真实 PostgreSQL 二进制

# 一条命令同时拉起内嵌 PostgreSQL + uvicorn
python scripts/serve_local.py
#   API/Swagger: http://127.0.0.1:8000/docs

# 另开一个终端，跑「同名改绑 + 限额批次」全流程实测脚本（33 项断言）
python scripts/demo_http.py
```

也可以对任意已运行的实例（包括 compose 起的实例）运行：

```bash
BASE_URL=http://localhost:8000 python scripts/demo_http.py
```

### 跑测试

```bash
pip install -r requirements-dev.txt
pytest -q
# 29 passed
```

测试默认通过 `pgserver` 自动启动一个真实 PostgreSQL 16；若设置了
`DATABASE_URL` 则改用外部库（包括 compose 的库）。

---

## 2. 在 `/docs` 管理「会话 / 查询名」绑定

打开 <http://localhost:8000/docs>，找到
`PUT /api/sessions/{session}/bindings/{name}`，直接 **Try it out**：

- 同名查询在不同 `session` 下 **完全独立**（存储键是 `(session, name)`）。
- 再次 `PUT` 同一 `(session, name)` 即 **改绑**，`version` 自增。
- SQL 两种来源：
  1. `template`：内置的按 **服务 / 版本 / 日期** 筛选的参数化 SQL
     （见 `GET /api/templates`，共 6 个，含组合筛选与日期区间）；
  2. `sql`：自己的参数化 SQL，支持 `$1`、`:name`、`%(name)s` 三种占位符，
     统一规范化成 `$n`。

```bash
# 会话 alice 下，名字 rpt 绑定「按服务」模板
curl -X PUT http://localhost:8000/api/sessions/alice/bindings/rpt \
  -H 'Content-Type: application/json' \
  -d '{"template":"deployments_by_service"}'

# 改绑成「按日期」
curl -X PUT http://localhost:8000/api/sessions/alice/bindings/rpt \
  -H 'Content-Type: application/json' \
  -d '{"template":"deployments_by_date"}'

# 另一个会话的同名 rpt，互不影响
curl -X PUT http://localhost:8000/api/sessions/bob/bindings/rpt \
  -H 'Content-Type: application/json' \
  -d '{"template":"deployments_by_version"}'
```

内置模板：

| template id | 筛选维度 | 参数 |
| --- | --- | --- |
| `deployments_by_service` | 服务 | `service` |
| `deployments_by_version` | 版本 | `version` |
| `deployments_by_date` | 日期（`deployed_at::date`） | `date` |
| `deployments_by_service_and_version` | 服务+版本 | `service`,`version` |
| `deployments_between_dates` | 日期区间 | `date_from`,`date_to` |
| `deployments_filter` | 三者任意组合，可空 | `service`,`version`,`date` |

---

## 3. 提交一批：排程 + 限额 + 真实取数

`POST /api/batches`（Swagger 里有完整示例）。一批 **1–8** 个查询，每个查询填：

| 字段（含兼容别名） | 含义 |
| --- | --- |
| `session` / `session_id` | 会话 |
| `name` / `query_name` | 查询名 |
| `params` / `parameters` / `args` | 参数：数组、命名对象，单参数可直接给标量 |
| `arrival_tick` / `arrival` / `release_tick` … | 到达拍 |
| `duration_ticks` / `duration` / `occupy_ticks` … | 独占连接的占用拍数 |
| `deadline_tick` / `deadline` / `latest_finish_tick` … | 最迟完成拍 |
| 顶层 `max_prepares` / `prepare_limit` / `prepare_budget` … | **全批 PREPARE 次数上限** |

拍的口径（响应里也会写明）：查询从 `start = max(arrival, 连接空闲拍)` 开始，
`end = start + duration`，标准口径要求 **`end <= deadline`**；此时
`last_busy_tick = end - 1`。若把 `inclusive_deadline=true`（整批或单条），
则改为要求 `last_busy_tick <= deadline`（即 `end <= deadline+1`）。

```bash
curl -X POST http://localhost:8000/api/batches \
  -H 'Content-Type: application/json' \
  -d '{
    "max_prepares": 1,
    "queries": [
      {"session":"alice","name":"rpt","params":{"date":"2026-11-10"},
       "arrival_tick":0,"duration_ticks":1,"deadline_tick":3},
      {"session":"bob","name":"rpt","params":{"version":"v2.0.1"},
       "arrival_tick":0,"duration_ticks":1,"deadline_tick":3}
    ]
  }'
```

判定与返回：

- **可受理**：HTTP 200，`status=completed`，按排程在真实 PG 上执行；每个查询返回
  `connection` / `connection_slot`、`backend_pid`、`start_tick` / `end_tick` /
  `completion_tick` / `last_busy_tick`、`cache_hit`、`prepare_count`（0 或 1）、
  `statement_name`、`columns` / `rows` / `records` / `row_count`。
  顶层还有 `actual_prepares`、`planned_prepares`、`makespan_ticks`、
  `plan.steps`、受理时冻结的 `snapshot`、执行前后的 `cache_before/after`。
- **无解**：HTTP **422**，`status=rejected`、`accepted=false`，给出
  `rejection.reason`：
  - `prepare_budget_exceeded`：存在满足时限的排程，但
    `minimum_prepares_among_deadline_feasible > max_prepares`，并附上该最优排程；
  - `deadline_infeasible`：即使 PREPARE 不限量，时限也不可能满足。
  - 拒绝发生在任何真实 `PREPARE` **之前**，只做 `pg_prepared_statements` 只读核对，
    因此 **原缓存照旧**（有测试逐字比对）。
- **执行出错**：HTTP **500**，`status=error`，返回 PostgreSQL `sqlstate` 与错误；
  本批立即停止，排程中位于失败步之后的查询标记为 `skipped`；随后对 **两条**
  连接查询 `pg_prepared_statements` 做核对，以连接上仍真实存在的预备语句为准
  重建缓存元数据。
- **已有批次在跑**：HTTP **409** `batch_busy`（进程级 `threading.Lock`
  非阻塞获取，保证一次只跑一批）。
- `POST /api/batches/plan`：只排程试算，不执行、不发 `PREPARE`、不动缓存。
- `POST /api/batches/submit`：信封风格，**始终 HTTP 200**，用 `ok/status` 判断。
- 另有根路径别名 `/batches`、`/sessions`、`/cache` 等（不出现在 `/docs` 中，
  保持 Swagger 整洁）。

---

## 4. 实现说明

### 4.1 排程器：小规模下的精确枚举（`app/planner.py`）

每批至多 8 个查询，因此直接枚举：

- 每个查询分配到两条连接：`2^n` 个分区；
- 每个分区内枚举两条连接上的排列：`k! · (n-k)!`；
- 合计候选排程数

$$\sum_{k=0}^{n}\binom{n}{k}k!(n-k)!=(n+1)n!\le 9\cdot 40320=362\,880$$

每个连接按给定顺序做最早开始模拟：`start=max(arrival,free_at)`，检查
`start+duration<=deadline`，同时用 **LRU** 模拟容量 2 的预备语句缓存：
命中 → 0 PREPARE 并移到 MRU；未命中 → 1 PREPARE，加入后超过 2 条就淘汰 LRU。

在所有「时限可行」的候选中，按以下键选全局最优（确定性平局规则）：

```
(PREPARE 总数, 最大完成拍 makespan, 完成拍之和, 分配掩码, 两连接顺序)
```

即先帮你 **省 PREPARE**，再尽量提前完工。这自然刻画了核心权衡：把查询派给
「恰好缓存了对应 SQL 的连接」可以命中，但如果它在排队、会错过 deadline，
排程器就会选择另一条空闲连接（未命中、多一次 PREPARE）来保时限。

实测最坏情况 n=8、全部时限宽松的穷举约 **1.2 s**；时限紧张时大量分支被提前剪枝，
约 **0.1 s**。

### 4.2 两条真实连接与真实 PREPARE/EXECUTE（`app/runtime.py`）

- `BatchRuntime` 启动时恰好创建 **2 条** autocommit 的 psycopg 3 长连接，
  设置 `prepare_threshold=None` 关闭 psycopg 自身的自动预备，保证观察到的
  PREPARE 全是本服务显式发出的。
- 缓存身份是 **规范化 SQL 文本的 SHA1 前 16 位**，物理语句名 `rq_<hash>`
  （同 SQL 跨会话/查询名也会正确共享命中；改绑成新 SQL 会得到新 hash）。
- 未命中时执行真实的 `PREPARE rq_x AS <sql>`；取数执行
  `EXECUTE rq_x(<安全字面量...>)`。参数通过 `psycopg.sql.Literal` 适配/引用
  （dict 自动包成 `Jsonb`，list 走 PG 数组），不存在字符串拼接注入。
- 每查询的 `backend_pid` 直接取自 `conn.info.backend_pid`，并与
  `/api/connections` 中两条连接的 PID 对得上。
- 超过 2 条时对 LRU 语句执行真实 `DEALLOCATE`。
- **跨批保留**：`ConnectionSlot.statements` 与后端 session 内的预备语句都一直存活。
- **出错核对**：任何一步抛错 → 停止循环 → 对每条连接查
  `pg_prepared_statements WHERE left(name,3)='rq_'`，元数据中失效的条目剔除、
  PG 中存在但元数据缺失的语句补登；连接已断则丢弃并按需重连（新后端缓存为空，
  这就是新连接的事实）。

> 说明：我们用显式 SQL 级 `PREPARE/EXECUTE` 而不是 psycopg 隐式预备，是为了让
> 「真实 PREPARE 次数」「每连接两条」「按名核对 `pg_prepared_statements`」
> 都完全可见、可控、可测。

### 4.3 绑定快照（`app/bindings.py`、`app/routes.py`）

- `BindingStore` 是线程安全的内存存储，键 `(session,name)`，保存规范化 SQL、
  参数名、hash、版本号。
- 提交批次时用 `find_many` 在 **同一把锁** 内取得整批一致的绑定，生成不可变
  `ResolvedQuery` 与响应/批次记录里的 `snapshot`；之后即使立刻改绑，本批执行与
  历史详情仍用旧 SQL/旧版本。

### 4.4 内置部署记录（`app/seed.py`）

首次启动幂等建表 `deployments` 并灌入 84 条确定性数据（2026-11-05 ~ 11-25，
每天 4 个服务，含版本、环境、状态、部署时间、发布人），保证按服务/版本/日期
筛选都有非空且可断言的结果（例如 `2026-11-10` 恰好 4 条）。

### 4.5 HTTP 面（`app/main.py`、`app/routes.py`、`app/models.py`）

- 规范入口前缀 `/api`，全部进入 `/docs`；同一路由器再无根前缀挂载一份作为别名，
  但用 `include_in_schema=False` 隐藏，避免 Swagger 重复。
- 输入模型用 Pydantic `AliasChoices` 兼容常见字段命名。
- 主要端点：

```
PUT/POST /api/sessions/{session}/bindings[/{name}]
GET      /api/sessions , /api/sessions/{s}/bindings[/{n}]
DELETE   /api/sessions/{s}/bindings/{n}
GET      /api/bindings , POST/PUT /api/bindings
GET      /api/templates
POST     /api/batches , /api/batches/plan , /api/batches/submit
GET      /api/batches , /api/batches/{id}
GET      /api/cache , POST /api/cache/reconcile
GET      /api/connections
GET      /api/deployments?service=&version=&date=
GET      /health , /docs , /openapi.json
```

---

## 5. 实测结果（本仓库编写时真实执行）

环境：Linux x86_64 / Python 3.11.2 / FastAPI 0.165.0 / psycopg 3.3.9 /
Pydantic 2.14.4 / uvicorn 0.54.0；测试与本地演练通过 `pgserver 0.1.4`
拉起 **真实 PostgreSQL 16.2**（非 mock）。

### 5.1 自动化测试

```text
$ pytest -q
29 passed, 2 warnings in 1.90s
```

- `tests/test_planner.py`（10）：冷缓存 0 限额、最小 PREPARE 数、热缓存 0 限额、
  物理时限不可行、PREPARE 换并行保 deadline、到达拍、截止拍边界（含 inclusive）、
  LRU 淘汰导致重新 PREPARE、跨批 LRU、8 查询穷举性能。
- `tests/test_api_e2e.py`（19）：`/docs`/OpenAPI/health、种子数据三种筛选、
  同名跨会话独立、改绑版本与旧 SQL、三种占位符与标量/对象/数组参数、
  缺失绑定 404、真实 PID/起止拍/结果、双连接并行、**受理快照抗改绑**、
  **限额拒绝且缓存逐字不变**、限额受理后 0 限额全命中、
  **每连接容量 2 的跨批 LRU 淘汰**、**执行出错停止 + sqlstate + 核对恢复**、
  超 8 条 422、**持锁时 409**、试算不动缓存、信封与根别名。

### 5.2 同名改绑 + 限额批次真机演练

```text
$ python scripts/serve_local.py        # 内嵌 PG + uvicorn
$ python scripts/demo_http.py
...
=== 3. REBIND same session/name; old batch keeps its snapshot ===
  [PASS] rebind alice/rpt -> date template bumps v1->v2
    #0 alice/rpt pg-conn-2 pid=... ticks=0->1 prepare=1 hit=False rows=4
    #1 bob/rpt   pg-conn-1 pid=... ticks=0->1 prepare=0 hit=True  rows=7
  [PASS] only 1 new PREPARE (bob's version statement reused)
  [PASS] alice date rebind returns exactly 4 rows for 2026-11-10
  [PASS] bob same-name binding still version SQL and was a cache hit
  [PASS] stored B1 snapshot still v1 service SQL

=== 4. PREPARE-limit batch: reject leaves cache untouched ===
  [PASS] budget=2 -> HTTP 422 rejected
  [PASS] diagnostic says deadline-feasible needs 3 PREPARE
  [PASS] cache exactly unchanged after no-solution
  [PASS] budget=3 -> accepted & completed
  [PASS] actual PREPARE count == 3
  [PASS] repeat same batch with budget=0 -> 200
  [PASS] repeat batch needs 0 PREPARE and all hits

=== 5. deadline vs cached-connection trade-off ===
  [PASS] budget=0: waiting for the single cached conn misses deadlines -> 422
  [PASS] budget=1: fresh PREPARE on second conn makes deadlines -> 200
    #1 trade/same pg-conn-1 pid=... ticks=0->2 prepare=0 hit=True
    #0 trade/same pg-conn-2 pid=... ticks=0->2 prepare=1 hit=False
    #2 trade/same pg-conn-1 pid=... ticks=2->4 prepare=0 hit=True

=== 6. execution error stops batch and cache reconciles ===
  [PASS] bad SQL -> HTTP 500 error report
  [PASS] PostgreSQL sqlstate 42P01 surfaced
  [PASS] batch stops: one error, later schedule steps skipped ...
  [PASS] reconcile endpoint ok and cap respected
  [PASS] service still takes a good batch after the error
...
DEMO EXIT=0，33 项 [PASS]，0 失败
```

另外验证了镜像启动命令本身：带 `DATABASE_URL` 直接执行 Dockerfile 的 CMD
`uvicorn app.main:app --host 0.0.0.0 --port 8000` 可正常启动、`/health=ok`。

---

## 6. 目录结构

```text
app/
  main.py        # FastAPI、lifespan、/ 与 /health、双前缀挂载
  routes.py      # 全部 HTTP 端点与批次编排
  models.py      # Pydantic 输入模型（多别名兼容）
  bindings.py    # 线程安全的 (session,name) 绑定存储与快照
  templates.py   # 6 个按服务/版本/日期筛选的内置参数化 SQL
  sqlnorm.py     # $1 / :name / %(name)s -> 规范 $n，hash 与单语句校验
  planner.py     # 双连接 + LRU(2) 的精确枚举排程器（纯函数）
  runtime.py     # 2 条 psycopg 连接、真实 PREPARE/EXECUTE、出错核对
  seed.py        # deployments 建表 + 84 条内置部署记录
  store.py       # 内存批次记录
  config.py      # 环境变量配置
tests/           # 29 个 pytest（planner 单测 + 真实 PG 的 HTTP E2E）
scripts/
  serve_local.py # pgserver + uvicorn 本地一键
  demo_http.py   # 同名改绑 / 限额 / 权衡 / 出错恢复 实测脚本（仅标准库）
Dockerfile
docker-compose.yml
requirements.txt / requirements-dev.txt
```

---

## 7. 缺项与边界（如实说明）

1. **Docker 在本沙箱未实跑**：当前环境没有 `docker`/`dockerd` 也没有 sudo，
   因此 `docker compose up --build` 未能在这里真正执行；已做的验证是
   compose YAML 解析通过、配置按 v2 规范编写，并用等价的
   `uvicorn app.main:app` 命令实跑验证。镜像首次构建（pip 安装）未在本机实测，
   但锁定版本均为本机已从 PyPI 实际安装的版本，`psycopg[binary]` 提供
   slim/x86_64/arm64 预编译 wheel。
2. **「拍」是逻辑模拟时间**：不会按拍真实 sleep，真实取数耗时也不反馈回排程；
   排程保证的是给定 `duration_ticks` 模型下的时限可行性。
3. **真实执行按全局拍序串行发起**：两条连接是不同物理后端/PID，独占与顺序约束
   都被遵守，但没有用两个 OS 线程在墙钟时间上真正并行两个 EXECUTE
   （演练数据量很小，结果等价；若要更逼真可改成每连接一个 worker 并发执行）。
4. **绑定与批次记录仅存内存**：应用/容器重启后需要重新 PUT 绑定、批次历史清空；
   `deployments` 数据在 compose 的 `pgdata` 卷中持久化；预备语句依附于两条长连接，
   应用重启后自然为空并会按需重新 PREPARE。
5. **SQL 面向可信内网演练**：只做了「单条语句」校验，没有强制只读角色/白名单/
   鉴权。生产化建议使用只读 DB 角色，并以 `EXPLAIN`/解析器禁止
   INSERT/UPDATE/DELETE/DDL 与数据修改型 CTE。
6. **排程器为精确枚举，上限 8**：超过 8 个查询需要换 DP/约束求解；接口层已强制
   `max_length=8`。
7. 没有配 CI workflow，也没有用真实浏览器点 Swagger UI（仅验证 `/docs` 200 与
   OpenAPI 内容）。
