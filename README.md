# neo-infer

知识图谱规则挖掘与推理服务（MVP）。

## 当前实现范围
- 规则挖掘：长度为 2/3 的路径规则  
  - `r1(X,Z) ∧ r2(Z,Y) -> r3(X,Y)`
  - `r1(X,A) ∧ r2(A,B) ∧ r3(B,Y) -> r4(X,Y)`
- 指标：`support`、`pca_confidence`、`head_coverage`
- AMIE+ 搜索骨架与剪枝（已接入）：
  - dangling -> closing 分层搜索
  - canonical 去重、support/no-gain 剪枝、confidence upper-bound 预剪枝
  - `beam_width`（层级 Top-B）与 `head_budget_per_relation`（按 head 配额）
- 规则管理：`discovered / adopted / applied / rejected`，带状态机校验
- 规则应用：已采纳规则单轮/fixpoint 推理，写入 `is_inferred=true` 的关系
- 冲突管理：数据库持久化冲突规则 + 冲突实例落库（`ConflictCase`）
- 增量挖掘：`ChangeLog(change_seq)` 驱动，支持 from-changelog 非空/混合 add-remove/幂等消费
- Web 控制台：轻量浏览器界面，包含规则挖掘/管理/推理/增量消费全流程操作
- Trigger 管理：兼容 Neo4j 5.x（`apoc.trigger.install/show/drop`）与 4.x（`apoc.trigger.add/list/remove`）

## 项目结构
```text
neo_infer/
  config.py              # 运行配置
  models.py              # Pydantic 数据模型
  db.py                  # Neo4j 连接与执行
  query.py               # Cypher 查询模板
  rule_mining.py         # 挖掘服务（AMIE+ 搜索框架）
  rule_management.py     # 规则管理服务（含状态机校验）
  inference.py           # 推理引擎（单轮 + fixpoint）
  conflict_management.py # 冲突规则与实例管理
  incremental_mining.py  # 增量挖掘服务
  incremental_store.py   # ChangeLog / 游标 / RuleStat 持久层
  trigger_management.py  # APOC Trigger 生命周期管理
  cli.py                 # 命令行工具
  api.py                 # FastAPI 路由 + Web 控制台
main.py                  # 应用入口
tests/                   # pytest smoke tests（35 个用例）
scripts/                 # 压测与 schema 工具脚本
PLAN                     # 总体实施计划
AGENTS.md                # Cloud Agent 开发环境说明
```

## 环境变量
- `NEO4J_URI`（默认 `bolt://localhost:7687`）
- `NEO4J_USER`（默认 `neo4j`）
- `NEO4J_PASSWORD`（默认 `neo4j`）
- `NEO4J_DATABASE`（默认 `neo4j`）
- `MIN_SUPPORT`（默认 `5`）
- `MIN_CONFIDENCE`（默认 `0.1`）
- `MAX_RULE_LENGTH`（默认 `2`）
- `CONFLICT_RELATION_PAIRS`（可选兜底配置，格式：`headRel:conflictRel,relA:relB`）
- `CHANGELOG_TRIGGER_AUTO_INSTALL`（默认 `0`，设为 `1` 则启动时自动安装 changelog trigger）
- `CHANGELOG_TRIGGER_NAME`（默认 `neo_infer_changelog`）

## Neo4j / APOC 前置要求
### 1) Neo4j 版本
- 建议 Neo4j 5.x（当前 Trigger 管理优先适配 `apoc.trigger.install/drop`）。
- 社区版可用；`SHOW SERVERS` 在社区版不支持，属于正常现象。

### 2) APOC 插件
- 需要安装与 Neo4j **同大版本**的 APOC（例如 Neo4j 5.x 对应 APOC 5.x）。
- 将 APOC jar 放到 Neo4j `plugins/` 目录，重启 Neo4j。

### 3) 配置项（neo4j.conf / apoc.conf）
- 允许 APOC procedure：
  - `dbms.security.procedures.allowlist=apoc.*`
  - `dbms.security.procedures.unrestricted=apoc.*`
- 开启 trigger：
  - `apoc.trigger.enabled=true`

### 4) 基础连通性验证
在 Neo4j Browser 或 cypher-shell 中执行：
```cypher
RETURN apoc.version();
CALL apoc.help("trigger");
-- Neo4j 5.x:
CALL apoc.trigger.show('neo4j');
-- Neo4j 4.x:
CALL apoc.trigger.list();
```

### 5) Trigger 安装方式
- API 手动安装（推荐）：
  - `POST /triggers/changelog/install`
- API 卸载：
  - `DELETE /triggers/changelog`
- 自动安装（可选）：
  - 设置 `CHANGELOG_TRIGGER_AUTO_INSTALL=1`，服务启动时自动尝试安装。

### 6) 常见问题
- 若安装时报 `No write operations are allowed ... FOLLOWER`：
  - 通常是连接到了不可写节点；单机请确认 `NEO4J_URI` 指向本机可写 bolt 端点（如 `bolt://127.0.0.1:7687`）。
- 若 API 返回“installed”但查询不到 trigger：
  - Neo4j 5.x 请用 `CALL apoc.trigger.show('neo4j')` 在 system 库查询（非 `apoc.trigger.list()`）。
  - 当前服务已自动适配 5.x/4.x 两种查询 API。

## 运行
```bash
pip install -e .
uvicorn main:app --reload
```

## 更简单交互（CLI + Web 控制台）
### 1) 轻量 CLI
安装后可直接使用：
```bash
neo-infer --help
neo-infer health
neo-infer mine --body-length 2 --limit 100 --min-support 1 --min-pca-confidence 0.1
neo-infer rules list --status discovered --limit 50
neo-infer infer --limit-rules 100 --fixpoint --max-iterations 5
neo-infer changes append --add "u1,bornIn,u2" --add "u2,locatedIn,u3"
neo-infer incremental consume --body-length 2 --limit 100 --change-limit 1000
neo-infer trigger install
```

如 API 不在本机 `8000` 端口，可加全局参数：
```bash
neo-infer --api-base http://127.0.0.1:9000 health
```

### 2) 轻量 Web 控制台
服务启动后浏览器打开：
```text
http://127.0.0.1:8000/console
```
控制台提供常用操作按钮与参数输入：
- Health / 规则列表 / 冲突实例查询
- 规则挖掘（`/rules/mine`）
- **规则管理**（Rules Management）：规则表格 + 状态筛选 + 每条规则的 Adopt/Reject 按钮 + Adopt All 批量操作
- 推理执行（`/inference/run`）
- 变更追加（`/changes/append`）
- 增量消费（`/rules/mine/incremental/from-changelog`）

## 规则状态机
规则状态转换由服务端强制校验，非法转换返回 `409 Conflict`。

```text
discovered --[adopt]--> adopted --[inference]--> applied (终态)
discovered --[reject]--> rejected (终态)
adopted    --[reject]--> rejected (终态)
```

- `adopt`：仅接受 `discovered` 状态
- `reject`：接受 `discovered` 或 `adopted` 状态
- `applied`：仅由推理引擎内部设置（`created_triples > 0` 时自动转换），终态
- `rejected`：终态，不可再转换

| 当前状态 | adopt | reject | inference |
|---------|-------|--------|-----------|
| discovered | adopted | rejected | - |
| adopted | 409 | rejected | applied |
| applied | 409 | 409 | - |
| rejected | 409 | 409 | - |

## 本地 Neo4j 完整测试流程（不含 Docker）
以下流程假设你已在本地启动并可访问 Neo4j。

### 1) 准备测试数据（可选但推荐）
```bash
cypher-shell -a bolt://127.0.0.1:7687 -u neo4j -p neo4j '
MATCH (n) DETACH DELETE n;
CREATE (alice:Entity {id:"alice"}), (bob:Entity {id:"bob"}),
       (beijing:Entity {id:"beijing"}), (shanghai:Entity {id:"shanghai"}),
       (china:Entity {id:"china"}), (asia:Entity {id:"asia"});
CREATE (alice)-[:bornIn]->(beijing),
       (bob)-[:bornIn]->(shanghai),
       (beijing)-[:locatedIn]->(china),
       (shanghai)-[:locatedIn]->(china),
       (china)-[:partOf]->(asia),
       (alice)-[:nationality]->(china),
       (alice)-[:region]->(asia),
       (bob)-[:noNationality]->(china);'
```

### 2) 设置环境并启动服务
```bash
export NEO4J_URI="bolt://127.0.0.1:7687"
export NEO4J_USER="neo4j"
export NEO4J_PASSWORD="neo4j"
export NEO4J_DATABASE="neo4j"
pip install -e ".[dev]"
uvicorn main:app --reload
```

### 3) 健康检查
```bash
curl "http://127.0.0.1:8000/health"
```

### 4) 规则挖掘（length2 + length3）
```bash
curl -X POST "http://127.0.0.1:8000/rules/mine" \
  -H "Content-Type: application/json" \
  -d '{"body_length":2,"limit":100,"min_support":1,"min_pca_confidence":0.1}'

curl -X POST "http://127.0.0.1:8000/rules/mine/length3" \
  -H "Content-Type: application/json" \
  -d '{"body_length":3,"limit":100,"min_support":1,"min_pca_confidence":0.1}'
```

### 5) 采纳规则并执行推理
先查看规则并复制一个 `rule_id`：
```bash
curl "http://127.0.0.1:8000/rules?status=discovered&limit=100"
```

采纳并推理（示例把 `<RULE_ID>` 替换为真实值）：
```bash
curl -X POST "http://127.0.0.1:8000/rules/<RULE_ID>/adopt"

curl -X POST "http://127.0.0.1:8000/inference/run" \
  -H "Content-Type: application/json" \
  -d '{"limit_rules":100,"fixpoint":false,"max_iterations":5,"check_conflicts":false}'
```

> **注意**：`adopt`/`reject` 接口带状态机校验。规则不存在返回 `404`，非法转换返回 `409`。
> 合法转换路径见下方"规则状态机"一节。

### 6) 冲突链路验证（含冲突实例）
```bash
curl -X PUT "http://127.0.0.1:8000/conflicts" \
  -H "Content-Type: application/json" \
  -d '{"pairs":{"nationality":["noNationality"]}}'

curl -X POST "http://127.0.0.1:8000/inference/run" \
  -H "Content-Type: application/json" \
  -d '{"limit_rules":100,"fixpoint":false,"check_conflicts":true}'

curl "http://127.0.0.1:8000/conflicts/cases?limit=50"
curl "http://127.0.0.1:8000/conflict-cases?limit=50"
```

### 7) 增量挖掘链路验证（from-changelog）
追加变更：
```bash
curl -X POST "http://127.0.0.1:8000/changes/append" \
  -H "Content-Type: application/json" \
  -d '{
    "added_edges":[
      {"src":"u1","rel":"bornIn","dst":"u2"},
      {"src":"u2","rel":"locatedIn","dst":"u3"}
    ],
    "removed_edges":[]
  }'
```

首次消费（非空 delta）：
```bash
curl -X POST "http://127.0.0.1:8000/rules/mine/incremental/from-changelog" \
  -H "Content-Type: application/json" \
  -d '{"limit":100,"min_support":1,"min_pca_confidence":0.1,"body_length":2}'
```

再次消费（幂等，期望 `processed_changes=0`）：
```bash
curl -X POST "http://127.0.0.1:8000/rules/mine/incremental/from-changelog" \
  -H "Content-Type: application/json" \
  -d '{"limit":100,"min_support":1,"min_pca_confidence":0.1,"body_length":2}'
```

混合 add/remove：
```bash
curl -X POST "http://127.0.0.1:8000/changes/append" \
  -H "Content-Type: application/json" \
  -d '{
    "added_edges":[{"src":"u10","rel":"bornIn","dst":"u20"}],
    "removed_edges":[{"src":"u2","rel":"locatedIn","dst":"u3"}]
  }'
```

```bash
curl -X POST "http://127.0.0.1:8000/rules/mine/incremental/from-changelog" \
  -H "Content-Type: application/json" \
  -d '{"limit":100,"min_support":1,"min_pca_confidence":0.1,"body_length":2}'
```

## 自动化稳定性测试（Smoke）
API 全链路 smoke tests（35 个用例），覆盖：
- `rules mine`（长度2/长度3/增量）
- `inference`（单轮/fixpoint）
- `conflicts`（增删改查 + case 查询）
- **规则状态转换**（10 个用例：合法路径、非法转换 409、不存在 404）

运行命令：
```bash
pip install -e ".[dev]"
pytest -q
```

## 大批量性能压测与索引策略迭代
以下脚本用于你本地 Neo4j 场景下的性能实验（不依赖 Docker）。

### 1) 生成大规模基准数据
```bash
python scripts/bench_seed_large.py \
  --reset \
  --num-person 200000 \
  --num-city 5000 \
  --num-country 500 \
  --num-region 200 \
  --batch-size 5000
```

说明：
- 脚本会构造可用于规则挖掘/推理的结构化图（`bornIn/locatedIn/partOf/nationality/noNationality/worksAt/educatedAt/livesIn/headquartersIn`）。
- 若遇到认证失败，可显式传参：
  - `--uri bolt://127.0.0.1:7687 --user neo4j --password <你的密码> --database neo4j`

### 2) 执行挖掘/推理基准
确保 API 已启动后执行：
```bash
python scripts/bench_api_perf.py \
  --api-base-url http://127.0.0.1:8000 \
  --health-retries 60 \
  --health-interval 2 \
  --body-length 2 \
  --mine-loops 3 \
  --infer-loops 3 \
  --top-k 1000 \
  --infer-limit-rules 500 \
  --output-json bench_api_perf.json
```

输出：
- 挖掘/推理/增量三段的 `mean/p95/max` 耗时
- 每次请求的状态码与关键计数（rule 数、processed_changes 等）
- JSON 结果可落盘用于对比（`--output-json` / `--out`）

基准建议：
- 先 `bench_seed_large.py --reset`，再执行挖掘压测，避免历史图状态影响结果。
- 若要评估“纯事实图”的规则质量，保持 `factual_only=true`（默认值）。
- 若要评估“事实+推理混合图”统计，可显式传 `factual_only=false`。

### 3) 索引策略自动对比
```bash
python scripts/bench_index_strategies.py \
  --api-base-url http://127.0.0.1:8000 \
  --body-length 2 \
  --mine-loops 3 \
  --infer-loops 3 \
  --output-json bench_index_compare.json
```

内置策略：
- `baseline`: 仅核心唯一约束
- `lean`: baseline + 常用单列索引
- `aggressive`: lean + 额外复合/高频查询索引

脚本会对每个策略：
1) 应用索引集  
2) 执行同一批压测请求  
3) 汇总并输出对比结果（便于迭代选择）

## Neo4j Schema（索引/约束）与迁移
- 应用启动时会自动执行 schema bootstrap（幂等）：
  - `Rule.rule_id` 唯一约束
  - `ConflictRule(head_relation, conflict_relation)` 复合唯一约束
  - `RelationType.name` 唯一约束
  - `RuleStat.rule_id` 唯一约束
  - `IncrementalState.name` 唯一约束
  - `IdSequence.name` 唯一约束
  - `ChangeLog.change_seq` 唯一约束
  - 常用查询索引（`Rule.head_relation`、`Rule.status`、`ChangeLog.rel`、`ChangeLog.event_type`、`ConflictCase.rule_id`）
- 也可手动执行脚本：
  - `scripts/neo4j_schema.cypher`
  - `python scripts/apply_neo4j_schema.py`

## ChangeLog 游标迁移（id() -> change_seq）
- 已从 `id(c)` 游标迁移到应用层 `ChangeLog.change_seq`（单调递增）。
- 增量消费与 pending 查询都基于 `change_seq`，避免 `id()` deprecation 风险。
- 迁移兼容：
  - schema 脚本会为历史 `ChangeLog` 回填 `change_seq`
  - 同步初始化 `IdSequence{name:'ChangeLog'}.next_seq`

## API
- `GET /health`
- `POST /rules/mine`
- `GET /rules?status=discovered&limit=100`
- `POST /rules/{rule_id}/adopt`
- `POST /rules/{rule_id}/reject`
- `GET /conflicts`
- `PUT /conflicts`
- `GET /conflicts/cases?limit=100`
- `POST /inference/run`
- `POST /changes/append`
- `POST /rules/mine/incremental/from-changelog`
- `POST /triggers/changelog/install`（安装/更新 DB Trigger）
- `DELETE /triggers/changelog`（卸载 DB Trigger）

### 挖掘参数说明（关键）
- `factual_only`（默认 `true`）：
  - 当为 `true` 时，规则挖掘相关统计（body 候选、support、PCA 分母、head 计数）仅使用事实边（`is_inferred != true`）。
  - 用于避免 benchmark 被历史推理边污染，建议保持默认值。
  - 如需包含推理边参与统计，可显式传 `false`。
- `beam_width`（可选）：
  - 每层 body 扩展仅保留 Top-B 候选，控制搜索空间。
- `head_budget_per_relation`（可选）：
  - 每个 head relation 最多保留 K 条候选规则，防止头关系垄断。
- `confidence_ub_weight`（默认 `0.0`）：
  - 基于局部统计收紧置信度上界（0~1），值越大预剪枝越激进。

## 变更日志混合模式（Trigger + App）
- 支持两种写入来源合并：
  - `source=app`：通过 `/changes/append` 显式提交；
  - `source=trigger`：由 APOC trigger 自动捕获 Neo4j 内部关系变更。
- 去重策略：
  - `ChangeLog.dedup_key` 唯一约束；
  - App 通道可带 `batch_id` / `idempotency_key` / `context`；
  - 增量消费窗口内会对同一 `(src, rel, dst)` 的 add/remove 进行折叠（同窗 add+remove 抵消）。
- Trigger 自过滤：
  - 默认跳过系统内部标签（`ChangeLog/IdSequence/IncrementalState/Rule/RuleStat/Conflict*`），避免自触发循环。
- 触发器与序号策略：
  - Trigger 回调不直接分配 `change_seq`（避免并发事务下唯一键冲突）；
  - `change_seq` 由应用侧在消费/查询前统一补齐并推进，保证游标稳定。
- 启用方式：
  - 自动安装（启动时）：`CHANGELOG_TRIGGER_AUTO_INSTALL=1`
  - 手动安装：`POST /triggers/changelog/install`
  - 手动卸载：`DELETE /triggers/changelog`

### ChangeLog 保留与清理建议
- 默认建议：**消费后保留**（便于审计、回放、排障）。
- 清理策略建议：
  - 按时间窗口（如保留最近 30 天）；
  - 或按消费游标与条数阈值（如仅保留最近 N 条已消费日志）。
- 不建议“消费即删”，否则会降低可追溯性。

## 推理接口说明（增强）
`POST /inference/run` 支持冲突检测字段：

```json
{
  "limit_rules": 100,
  "fixpoint": false,
  "max_iterations": 5,
  "check_conflicts": true,
  "conflict_pairs": {
    "nationality": ["noNationality"]
  }
}
```

- 冲突策略优先级：
  1) 请求体 `conflict_pairs`
  2) 数据库 `ConflictRule`（通过 `/conflicts` 管理）
  3) 环境变量 `CONFLICT_RELATION_PAIRS`（兼容兜底）
- 对每条待推理规则，若候选 `(X,Y)` 同时存在冲突关系，则计入冲突统计。
- 响应中会返回：
  - `results[].conflict_triples`
  - `total_conflicts`
- 冲突实例会记录到 `ConflictCase`：
  - `rule_id`
  - `inferred_relation`
  - `conflicting_relation`
  - `source_x / source_y`
  - `first_iteration / last_iteration / detect_count`

## 规则挖掘请求示例（长度2/3 + 增量）
```json
{
  "limit": 200,
  "body_length": 3,
  "min_support": 1,
  "min_pca_confidence": 0.2,
  "candidate_limit": 5000,
  "affected_relations": ["bornIn", "locatedIn"]
}
```

- `body_length`: `2` 或 `3`
- `affected_relations`: 可选，增量重算入口；仅挖掘 body/head 涉及这些关系的候选规则

## 真增量挖掘（ChangeLog 驱动）
新增两个核心接口：

1) 追加图变更日志：
```bash
curl -X POST http://127.0.0.1:8000/changes/append \
  -H "Content-Type: application/json" \
  -d '{
    "added_edges":[{"src":"1","dst":"2","rel":"bornIn"}],
    "removed_edges":[]
  }'
```

2) 消费日志并执行增量挖掘：
```bash
curl -X POST http://127.0.0.1:8000/rules/mine/incremental/from-changelog \
  -H "Content-Type: application/json" \
  -d '{"body_length":2,"limit":100,"min_support":1,"min_pca_confidence":0.1}'
```