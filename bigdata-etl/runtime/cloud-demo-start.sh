#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="${APP_ROOT:-/data/app}"
ETL_DIR="${ETL_DIR:-${APP_ROOT}/bigdata-etl}"
BACKEND_DIR="${BACKEND_DIR:-${APP_ROOT}/bigdata-backend}"
WEB_DIR="${WEB_DIR:-${APP_ROOT}/bigdata-webapp}"
DATA_DIR="${DATA_DIR:-/data/bigdata-demo}"
JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-alibaba-dragonwell-17.0.16.0.17.8-1.alnx4.x86_64}"
BACKEND_JAR="${BACKEND_JAR:-${BACKEND_DIR}/target/bigdata-backend-1.0.0-SNAPSHOT.jar}"
FLINK_JAR="${FLINK_JAR:-${ETL_DIR}/bigdata-etl-pipeline/target/bigdata-etl-pipeline-1.0.0-SNAPSHOT.jar}"
NETWORK="${NETWORK:-bigdata-demo-net}"
NETWORK_SUBNET="${NETWORK_SUBNET:-172.30.80.0/24}"
DORIS_FE_IP="${DORIS_FE_IP:-172.30.80.10}"
DORIS_BE_IP="${DORIS_BE_IP:-172.30.80.11}"

mkdir -p \
  "${DATA_DIR}/zookeeper" \
  "${DATA_DIR}/kafka" \
  "${DATA_DIR}/doris/fe/doris-meta" \
  "${DATA_DIR}/doris/fe/log" \
  "${DATA_DIR}/doris/be/storage" \
  "${DATA_DIR}/doris/be/log" \
  "${DATA_DIR}/logs"

stop_container() {
  docker rm -f "$1" >/dev/null 2>&1 || true
}

wait_http() {
  local name="$1"
  local url="$2"
  local max="${3:-90}"
  for _ in $(seq 1 "${max}"); do
    if curl -fsS --max-time 3 "${url}" >/dev/null 2>&1; then
      echo "[ok] ${name}: ${url}"
      return 0
    fi
    sleep 2
  done
  echo "[error] ${name} not ready: ${url}" >&2
  return 1
}

wait_mysql() {
  for _ in $(seq 1 120); do
    if mysql -h127.0.0.1 -P9030 -uroot -e "select 1" >/dev/null 2>&1; then
      echo "[ok] Doris query port: 9030"
      return 0
    fi
    sleep 2
  done
  echo "[error] Doris query port not ready" >&2
  return 1
}

wait_doris_backend() {
  for _ in $(seq 1 120); do
    if mysql -h127.0.0.1 -P9030 -uroot -N -B -e "show backends" 2>/dev/null | awk -F '\t' '$10 == "true" { found=1 } END { exit found ? 0 : 1 }'; then
      echo "[ok] Doris backend alive"
      return 0
    fi
    sleep 2
  done
  echo "[error] Doris backend not alive" >&2
  mysql -h127.0.0.1 -P9030 -uroot -e "show backends" >&2 || true
  return 1
}

echo "[1/9] Stop old demo containers"
for name in \
  bigdata-flink-realtime \
  bigdata-collector \
  bigdata-kafka-rest \
  bigdata-kafka \
  bigdata-zookeeper \
  bigdata-doris-be \
  bigdata-doris-fe; do
  stop_container "${name}"
done
docker network rm "${NETWORK}" >/dev/null 2>&1 || true
docker network create --subnet "${NETWORK_SUBNET}" "${NETWORK}" >/dev/null
rm -rf "${DATA_DIR}/zookeeper"/* "${DATA_DIR}/kafka"/*
chown -R 1000:1000 "${DATA_DIR}/zookeeper" "${DATA_DIR}/kafka"

echo "[2/9] Start Kafka stack"
docker run -d --name bigdata-zookeeper --network "${NETWORK}" --network-alias zookeeper \
  -p 2181:2181 \
  -e ZOOKEEPER_CLIENT_PORT=2181 \
  -e ZOOKEEPER_TICK_TIME=2000 \
  -v "${DATA_DIR}/zookeeper:/var/lib/zookeeper/data" \
  confluentinc/cp-zookeeper:7.6.1 >/dev/null

docker run -d --name bigdata-kafka --network "${NETWORK}" --network-alias kafka \
  -p 9092:9092 \
  -e KAFKA_BROKER_ID=1 \
  -e KAFKA_ZOOKEEPER_CONNECT=bigdata-zookeeper:2181 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://bigdata-kafka:29092,PLAINTEXT_HOST://127.0.0.1:9092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_LOG_RETENTION_HOURS=6 \
  -e KAFKA_LOG_SEGMENT_BYTES=67108864 \
  -e KAFKA_NUM_PARTITIONS=12 \
  -v "${DATA_DIR}/kafka:/var/lib/kafka/data" \
  confluentinc/cp-kafka:7.6.1 >/dev/null

docker run -d --name bigdata-kafka-rest --network "${NETWORK}" --network-alias kafka-rest \
  -p 8082:8082 \
  -e KAFKA_REST_HOST_NAME=bigdata-kafka-rest \
  -e KAFKA_REST_BOOTSTRAP_SERVERS=bigdata-kafka:29092 \
  -e KAFKA_REST_LISTENERS=http://0.0.0.0:8082 \
  confluentinc/cp-kafka-rest:7.6.1 >/dev/null

echo "[3/9] Start OpenResty collector"
docker run -d --name bigdata-collector --network "${NETWORK}" \
  -p 8081:8080 \
  -v "${ETL_DIR}/deploy/nginx/nginx.conf:/usr/local/openresty/nginx/conf/nginx.conf:ro" \
  -v "${ETL_DIR}/deploy/nginx/collect.lua:/etc/nginx/lua/collect.lua:ro" \
  openresty/openresty:1.25.3.2-0-alpine >/dev/null

echo "[4/9] Start Doris FE/BE"
docker run -d --name bigdata-doris-fe --hostname fe --network "${NETWORK}" --ip "${DORIS_FE_IP}" \
  -p 8030:8030 -p 9030:9030 -p 9010:9010 \
  -e FE_SERVERS=fe1:${DORIS_FE_IP}:9010 \
  -e FE_ID=1 \
  -v "${DATA_DIR}/doris/fe/doris-meta:/opt/apache-doris/fe/doris-meta" \
  -v "${DATA_DIR}/doris/fe/log:/opt/apache-doris/fe/log" \
  apache/doris:fe-2.1.8 >/dev/null

sleep 10

docker run -d --name bigdata-doris-be --hostname be --network "${NETWORK}" --ip "${DORIS_BE_IP}" \
  -p 8040:8040 -p 9050:9050 \
  -e FE_SERVERS=fe1:${DORIS_FE_IP}:9010 \
  -e BE_ADDR=${DORIS_BE_IP}:9050 \
  -v "${DATA_DIR}/doris/be/storage:/opt/apache-doris/be/storage" \
  -v "${DATA_DIR}/doris/be/log:/opt/apache-doris/be/log" \
  apache/doris:be-2.1.8 >/dev/null

echo "[5/9] Wait for middleware"
wait_http "Kafka REST" "http://127.0.0.1:8082/topics" 90
wait_http "Nginx Lua collector" "http://127.0.0.1:8081/health" 90
wait_mysql
wait_doris_backend

echo "[6/9] Init Kafka topics and Doris schema"
docker exec bigdata-kafka kafka-topics --bootstrap-server bigdata-kafka:29092 \
  --create --if-not-exists --topic behavior_raw --partitions 12 --replication-factor 1 >/dev/null
docker exec bigdata-kafka kafka-topics --bootstrap-server bigdata-kafka:29092 \
  --create --if-not-exists --topic behavior_late --partitions 12 --replication-factor 1 >/dev/null
mysql -h127.0.0.1 -P9030 -uroot < "${ETL_DIR}/deploy/doris/schema.sql"

echo "[7/9] Install backend systemd service"
cat >/etc/systemd/system/bigdata-backend.service <<SERVICE
[Unit]
Description=Bigdata Analytics Spring Boot Backend
After=network-online.target docker.service
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=${BACKEND_DIR}
Environment=JAVA_HOME=${JAVA_HOME}
Environment=PATH=${JAVA_HOME}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin
Environment=ANALYTICS_MOCK_ENABLED=false
Environment=ANALYTICS_FILE_ENABLED=false
Environment=DORIS_JDBC_URL=jdbc:mysql://127.0.0.1:9030/bigdata_analytics?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai
Environment=DORIS_USER=root
Environment=DORIS_PASSWORD=
Environment=INGEST_ENDPOINT=http://127.0.0.1:8081/collect
ExecStart=${JAVA_HOME}/bin/java -Xms256m -Xmx768m -jar ${BACKEND_JAR}
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
SERVICE
systemctl daemon-reload
systemctl enable --now bigdata-backend.service >/dev/null
systemctl restart bigdata-backend.service
wait_http "Backend API" "http://127.0.0.1:8088/api/analytics/realtime" 60

echo "[8/9] Configure nginx frontend"
cat >/etc/nginx/conf.d/bigdata-demo.conf <<NGINX
server {
    listen 80 default_server;
    server_name _;

    root ${WEB_DIR}/dist;
    index index.html;

    location / {
        try_files \$uri \$uri/ /index.html;
    }

    location /api/ {
        proxy_pass http://127.0.0.1:8088/api/;
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }

    location = /collect {
        proxy_pass http://127.0.0.1:8081/collect;
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
    }
}
NGINX
rm -f /etc/nginx/conf.d/default.conf
nginx -t
systemctl enable --now nginx >/dev/null
systemctl reload nginx || systemctl restart nginx

echo "[9/9] Start Flink realtime job"
docker run -d --name bigdata-flink-realtime --network "${NETWORK}" \
  -e TZ=Asia/Shanghai \
  -v "${ETL_DIR}:/workspace" -w /workspace \
  eclipse-temurin:17-jre \
  java \
  -Duser.timezone=Asia/Shanghai \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  -cp "${FLINK_JAR#/data/app/bigdata-etl/}" \
  com.bigdata.etl.pipeline.job.UserBehaviorRealtimeJob \
  --kafka.bootstrap.servers=bigdata-kafka:29092 \
  --kafka.group.id=bigdata-etl-realtime-cloud \
  --flink.parallelism=1 \
  --checkpoint.enabled=false \
  --doris.detail.load-url=http://bigdata-doris-be:8040/api/bigdata_analytics/dwd_event_detail/_stream_load \
  --doris.aggregate.load-url=http://bigdata-doris-be:8040/api/bigdata_analytics/dws_event_1m/_stream_load \
  --doris.detail.batch-size=50 \
  --doris.detail.flush-ms=1000 \
  --doris.aggregate.batch-size=20 \
  --doris.aggregate.flush-ms=1000 >/dev/null

sleep 8
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E 'bigdata-|NAMES'
curl -fsS --max-time 10 http://127.0.0.1/api/analytics/realtime >/dev/null
echo "[done] Bigdata demo is running."
