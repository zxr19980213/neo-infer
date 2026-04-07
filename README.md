# neo-infer

知识图谱规则挖掘与推理服务（MVP）。

## 当前实现范围
- 规则挖掘：长度为 2 的路径规则  
  `r1(X,Z) ∧ r2(Z,Y) -> r3(X,Y)`
- 指标：`support`、`pca_confidence`、`head_coverage`
- 规则管理：`discovered / adopted / applied / rejected`
- 规则应用：已采纳规则单轮推理，写入 `is_inferred=true` 的关系

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

## 运行
```bash
pip install -e .
uvicorn main:app --reload
```

## API
- `GET /health`
- `POST /rules/mine`
- `GET /rules?status=discovered&limit=100`
- `POST /rules/{rule_id}/adopt`
- `POST /rules/{rule_id}/reject`
- `POST /inference/run`