# bigdata-etl

用户行为分析实时计算工程，核心链路是 `Nginx + Lua -> Kafka -> Flink -> Doris`，缓存侧可接 Redis 或 Kvrocks。目标场景按日 10 亿埋点设计，工程里优先给出可运行骨架和关键扩展点。

## 模块

- `bigdata-etl-common`：行为事件、清洗事件、聚合模型、JSON 工具。
- `bigdata-etl-pipeline`：Flink DataStream 实时作业。
- `deploy/nginx`：OpenResty Lua 埋点接入口。
- `deploy/doris`：Doris DWD/DWS/ADS 表结构。
- `deploy/kafka`：Topic 创建脚本。

## 本地启动

```bash
docker compose up -d
bash deploy/kafka/create-topics.sh
mysql -h127.0.0.1 -P9030 -uroot < deploy/doris/schema.sql
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
mvn -pl bigdata-etl-pipeline -am package
flink run bigdata-etl-pipeline/target/bigdata-etl-pipeline-1.0.0-SNAPSHOT.jar
```

本地 Docker Compose 不包含 Doris 和 Flink 集群，建议使用已有环境或单独启动官方 Doris/Flink。Nginx 埋点入口是 `POST http://127.0.0.1:8080/collect`。

## 从零造数验证

当前本地端口因为 8080 被占用，OpenResty 暴露在 `8081`。要清空 Kafka/Doris 后模拟真实用户行为，并验证数据最终进入前端接口：

```bash
cd bigdata-etl
USERS=40 bash scripts/reset-and-seed-demo.sh
```

脚本会执行完整链路：清空 Doris 表、准备 Kafka topic、重启 Flink 实时作业并从最新 offset 消费、通过 Nginx Lua 批量上报 `page_view/product_view/add_cart/checkout/pay_success`，等待 Flink 写入 Doris DWD/DWS，然后请求 `http://127.0.0.1:5178/api/analytics/realtime` 验证前端可查。

如果只想在现有链路上追加一批埋点：

```bash
USERS=2000000 bash scripts/seed-events-only.sh
```

## 日 10 亿容量口径

- 日均约 11,574 条/秒，峰值按 5 到 10 倍准备，即 6 万到 12 万条/秒。
- Kafka `behavior_raw` 示例 96 分区，便于 Flink 并行度从 24 扩到 96。
- Flink 使用 Event Time Watermark，乱序默认 10 秒，迟到容忍 5 分钟。
- DWD 表用 Unique Key 按 `event_id, app_id` 幂等，DWS 表按 1 分钟窗口聚合。
- Doris Stream Load 默认明细 5000 条或 3 秒 flush，聚合 1000 条或 2 秒 flush。

## 生产扩展建议

- StateBackend 切换到 RocksDB + 远端 Checkpoint 路径，避免大状态压垮 TM 内存。
- 热点事件可以在 `appId|eventName` 后加随机盐做二阶段聚合，处理 `page_view` 倾斜。
- Redis/Kvrocks 可承载事件元数据、虚拟事件规则、实验分组和黑名单维表。
- ADS 漏斗可以用 Flink SQL Interval Join 或离线调度从 DWD 明细补算。
- 对 Doris 写入开启 label 规则和重试队列，失败批次投递 Kafka DLQ 方便回放。

更多 Flink 面试点映射见 [docs/flink-interview-points.md](docs/flink-interview-points.md)。
