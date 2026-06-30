# 部署说明

## 云端拓扑

```text
用户浏览器
  -> zhaobinjie.me
  -> 新加坡 2C2G 入口服务器
     - Nginx
     - HTTPS
     - 静态前端
     - /api 反向代理
  -> 国内 8C32G 演示服务器
     - Spring Boot
     - Nginx Lua Collector
     - Kafka
     - Flink
     - Doris
```

## 域名入口 Nginx 示例

```nginx
server {
    listen 80;
    server_name zhaobinjie.me www.zhaobinjie.me;
    return 301 https://$host$request_uri;
}

server {
    listen 443 ssl;
    http2 on;
    server_name zhaobinjie.me www.zhaobinjie.me;

    root /data/app/bigdata-webapp/dist;
    index index.html;

    location /api/ {
        proxy_pass http://YOUR_BACKEND_SERVER/api/;
        proxy_set_header Host YOUR_BACKEND_SERVER;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    location / {
        try_files $uri $uri/ /index.html;
    }
}
```

## 后端环境变量

```bash
export ANALYTICS_MOCK_ENABLED=false
export DORIS_JDBC_URL='jdbc:mysql://127.0.0.1:9030/bigdata_analytics?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai'
export DORIS_USER=root
export DORIS_PASSWORD=''
export INGEST_ENDPOINT='http://127.0.0.1:8080/collect'
export ANALYTICS_REALTIME_COUNTER_PATH='/data/app/bigdata-backend/runtime/realtime-visits.json'
```

## 构建部署

前端：

```bash
cd bigdata-webapp
npm install
npm run build
```

后端：

```bash
cd bigdata-backend
export JAVA_HOME=/path/to/jdk17
mvn package -DskipTests
```

大数据组件：

```bash
cd bigdata-etl
docker compose up -d
```

初始化 Doris：

```bash
mysql -h 127.0.0.1 -P 9030 -uroot < deploy/doris/schema.sql
```

启动 Flink 任务请参考 `bigdata-etl/runtime/cloud-demo-start.sh`。

