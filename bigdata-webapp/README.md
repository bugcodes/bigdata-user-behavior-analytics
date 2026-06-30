# bigdata-webapp

用户行为分析系统前端，Vue 3 + Vite + ECharts。默认代理到 `bigdata-backend` 的 `http://127.0.0.1:8088`。

## 启动

```bash
npm install
npm run dev
```

访问 `http://127.0.0.1:5178`。

## 功能

- 实时 PV、UV、DAU、TPS、P99 延迟、错误率。
- 最近 30 分钟趋势图。
- Top 事件表格。
- 今日转化漏斗。
- 一键调用后端模拟埋点上报到 Nginx Lua 入口。
