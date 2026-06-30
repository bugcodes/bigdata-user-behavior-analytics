# 架构说明

## 目标

该项目用于复刻用户行为分析系统的核心链路，目标是展示日 10 亿级埋点接入场景下的实时数仓设计能力。

## 分层

```text
采集层：Nginx Lua
消息层：Kafka
计算层：Flink
存储层：Doris
服务层：Spring Boot
展示层：Vue
```

## 数据流

1. 前端或造数任务发送行为事件。
2. Nginx Lua 做基础协议校验、补充接收时间、IP、User-Agent。
3. 事件写入 Kafka `behavior_raw`。
4. Flink 消费 Kafka，完成清洗、去重、Watermark 分配、分钟窗口聚合。
5. 明细写入 Doris `dwd_event_detail`。
6. 聚合写入 Doris `dws_event_1m`。
7. Spring Boot 查询 Doris，向 Vue 看板提供实时指标。

## PV/UV 口径

### 数仓口径

- PV：Doris `dws_event_1m` 中 `sum(pv)`。
- UV/DAU：Doris `dwd_event_detail` 中按 `user_id` 去重。
- 特点：链路完整，适合分析与审计，但受窗口、Watermark、Doris 写入可见性影响。

### 秒级旁路口径

- 首页访问时上报 `page_view`。
- Spring Boot 在转发到采集链路前，同步写入旁路实时计数。
- 返回字段：`fastPv`、`fastUv`。
- 特点：用户访问后秒级展示，适合门户首页指标。

## Flink 技术点

- Event Time
- Watermark
- Tumbling Window
- Keyed State 去重
- Side Output 迟到数据
- Checkpoint 容错
- Doris Stream Load Sink

