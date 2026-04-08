# neo-infer

知识图谱规则挖掘与推理服务（MVP）。

## 当前实现范围
- 规则挖掘：长度为 2/3 的路径规则  
  - `r1(X,Z) ∧ r2(Z,Y) -> r3(X,Y)`
  - `r1(X,A) ∧ r2(A,B) ∧ r3(B,Y) -> r4(X,Y)`
- 指标：`support`、`pca_confidence`、`head_coverage`
- 规则管理：`discovered / adopted / applied / rejected`
- 规则应用：已采纳规则单轮推理，写入 `is_inferred=true` 的关系
- 冲突管理：数据库持久化冲突规则 + 冲突实例落库（`ConflictCase`）

## 项目结构
```text
neo_infer/
  config.py          # 运行配置
  models.py          # Pydantic 数据模型
  db.py              # Neo4j 连接与执行
  query.py           # Cypher 查询模板
  rule_mining.py     # 挖掘服务
  rule_management.py # 规则管理服务
  inference.py       # 推理引擎
  api.py             # FastAPI 路由
main.py              # 应用入口
PLAN                 # 总体实施计划
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

## 运行
```bash
pip install -e .
uvicorn main:app --reload
```

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
新增了 API 全链路 smoke tests，覆盖：
- `rules mine`（长度2/长度3/增量）
- `inference`（单轮/fixpoint）
- `conflicts`（增删改查 + case 查询）

运行命令：
```bash
pip install pytest
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