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
- `POST /incremental/changelog/append`
- `POST /incremental/mine/consume`
- `GET /incremental/changelog/state`

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
  - `x_name / y_name`
  - `detected_at`

## 规则挖掘请求示例（长度2/3 + 增量）
```json
{
  "limit": 200,
  "rule_length": 3,
  "min_support": 1,
  "min_pca_confidence": 0.2,
  "candidate_limit": 5000,
  "affected_relations": ["bornIn", "locatedIn"]
}
```

- `rule_length`: `2` 或 `3`
- `affected_relations`: 可选，增量重算入口；仅挖掘 body/head 涉及这些关系的候选规则

## 真增量挖掘（ChangeLog 驱动）
新增三个接口：

1) 追加图变更日志：
```bash
curl -X POST http://127.0.0.1:8000/incremental/changelog/append \
  -H "Content-Type: application/json" \
  -d '{
    "added_edges":[{"src_id":"1","dst_id":"2","rel":"bornIn"}],
    "removed_edges":[]
  }'
```

2) 消费日志并执行增量挖掘：
```bash
curl -X POST http://127.0.0.1:8000/incremental/mine/consume \
  -H "Content-Type: application/json" \
  -d '{"body_length":2,"limit":100,"min_support":1,"min_pca_confidence":0.1}'
```

3) 查看游标：
```bash
curl http://127.0.0.1:8000/incremental/changelog/state
```