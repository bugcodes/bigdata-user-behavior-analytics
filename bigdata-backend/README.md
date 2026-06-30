# bigdata-backend

用户行为分析系统后端，JDK 17 + Spring Boot 3，面向前端看板提供实时指标查询、漏斗分析和测试埋点上报能力。

## 启动

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
mvn spring-boot:run
```

默认启用 `analytics.mock-enabled=true`，不依赖 Doris 也能返回演示数据。
首页访问 PV/UV 使用本地文件计数，便于小服务器演示；生产环境可以替换为 Redis/Kvrocks。

接入 Doris 时设置：

```bash
export ANALYTICS_MOCK_ENABLED=false
export DORIS_JDBC_URL='jdbc:mysql://127.0.0.1:9030/bigdata_analytics?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai'
```

## 接口

- `GET /api/analytics/realtime`：实时 PV、UV、DAU、TPS、P99 延迟、错误率、Top 事件、漏斗。
- `POST /api/analytics/trace`：向 Nginx Lua 埋点入口发送一条测试事件。
