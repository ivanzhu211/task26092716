# task26092716

文档抽取流水线的审计统计模块（Python 标准库 `sqlite3`，无外部依赖）。

## 结构

- `audit_stats/events.py` — 事件类型：`extracted` / `corrected` / `approved`
- `audit_stats/store.py` — `AuditStore`：SQLite 存储，追加事件时增量维护聚合表
- `audit_stats/naive.py` — `NaiveAudit`：纯内存朴素参照实现（测试基准）
- `tests/test_audit_pipeline.py` — 多进程并发压力测试

## 设计要点

- 原始事件存 `events` 表，`prune(now)` 以每事务 ≤1000 条的批次物理删除
  早于 `now − 365 天` 的事件，批次间提交，进程被kill后重跑可收敛。
- 查询全部走增量聚合表（`agg_vol` / `agg_hist` / `agg_acc`），因此 prune
  前后、甚至 prune 进行中（WAL 读写不互斥）结果都与朴素实现一致。
- 改正归属依赖每个 (doc, field) 的"热状态"（`st_extr` / `st_corr`）：
  只保留 72h 乱序窗口内的事件加窗口外最近一次抽取；窗口滑过后冷改正
  固化为永久标记、冷抽取坍缩成单条边界记录；文档 `approved` 后状态即删。
  因此越过保留期的日期，留存数据量不随当日事件数增长（未定稿文档的
  最小归属状态除外）。

## 运行测试

```
python3 -m unittest tests.test_audit_pipeline -v
```
