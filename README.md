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
- `INFERENCE_CONFLICT_RELATION`（可选，冲突关系名，例如 `notNationality`）

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

## 推理接口说明（增强）
`POST /inference/run` 支持冲突检测字段：

```json
{
  "limit_rules": 100,
  "fixpoint": false,
  "max_iterations": 5,
  "conflict_relation": "notNationality"
}
```

- 若设置了 `conflict_relation`，系统会在推理前检测：
  - body 匹配产生的 `(X,Y)` 中，已有 `X-[:conflict_relation]->Y` 的候选对
- 这些候选将被统计为冲突并跳过，不会创建 head 边
- 响应中新增：
  - `conflicts_detected`：冲突数量