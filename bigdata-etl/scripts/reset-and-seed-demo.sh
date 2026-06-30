#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INGEST_URL="${INGEST_URL:-http://127.0.0.1:8081/collect}"
FRONTEND_API="${FRONTEND_API:-http://127.0.0.1:5178/api/analytics/realtime}"
DORIS_HOST="${DORIS_HOST:-127.0.0.1}"
DORIS_QUERY_PORT="${DORIS_QUERY_PORT:-9030}"
DORIS_USER="${DORIS_USER:-root}"
DORIS_PASSWORD="${DORIS_PASSWORD:-}"
USERS="${USERS:-40}"
FLINK_GROUP_ID="bigdata-etl-realtime-demo-$(date +%s)"

mysql_cmd() {
  mysql -h"${DORIS_HOST}" -P"${DORIS_QUERY_PORT}" -u"${DORIS_USER}" ${DORIS_PASSWORD:+-p"${DORIS_PASSWORD}"} "$@"
}

wait_for_http() {
  local url="$1"
  local name="$2"
  for _ in $(seq 1 40); do
    if curl -fsS --max-time 3 "$url" >/dev/null 2>&1; then
      echo "[ok] ${name} ready: ${url}"
      return 0
    fi
    sleep 2
  done
  echo "[error] ${name} not ready: ${url}" >&2
  return 1
}

wait_for_doris() {
  for _ in $(seq 1 40); do
    if mysql_cmd -e "select 1" >/dev/null 2>&1; then
      echo "[ok] Doris query port ready"
      return 0
    fi
    sleep 2
  done
  echo "[error] Doris query port not ready" >&2
  return 1
}

send_event() {
  local event_id="$1"
  local user_id="$2"
  local device_id="$3"
  local session_id="$4"
  local event_name="$5"
  local event_time_ms="$6"
  local page="$7"
  local product_id="$8"
  local amount="$9"

  local payload
  payload="{
    \"eventId\":\"${event_id}\",
    \"appId\":\"demo-app\",
    \"userId\":\"${user_id}\",
    \"deviceId\":\"${device_id}\",
    \"sessionId\":\"${session_id}\",
    \"eventName\":\"${event_name}\",
    \"eventTime\":${event_time_ms},
    \"properties\":{
      \"page\":\"${page}\",
      \"productId\":\"${product_id}\",
      \"amount\":${amount},
      \"channel\":\"demo-seed\",
      \"campaign\":\"summer-live\"
    }
  }"

  for attempt in $(seq 1 5); do
    if curl -fsS --max-time 10 -X POST "${INGEST_URL}" \
      -H "Content-Type: application/json" \
      -d "${payload}" >/dev/null; then
      return 0
    fi
    sleep "$((attempt * 2))"
  done

  echo "[error] failed to send event after retries: ${event_id}" >&2
  return 1
}

cd "${PROJECT_DIR}"

echo "[1/7] Checking services"
wait_for_http "http://127.0.0.1:8081/health" "Nginx Lua collector"
wait_for_doris

echo "[2/7] Clearing Doris tables"
mysql_cmd <<SQL
use bigdata_analytics;
truncate table dwd_event_detail;
truncate table dws_event_1m;
truncate table ads_checkout_funnel;
SQL

echo "[3/7] Preparing Kafka topics"
docker rm -f bigdata-flink-realtime >/dev/null 2>&1 || true
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic behavior_raw --partitions 12 --replication-factor 1 >/dev/null
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic behavior_late --partitions 12 --replication-factor 1 >/dev/null
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --alter --topic behavior_raw --partitions 12 >/dev/null 2>&1 || true
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --alter --topic behavior_late --partitions 12 >/dev/null 2>&1 || true
sleep 5

echo "[4/7] Starting Flink realtime job (${FLINK_GROUP_ID})"
docker network connect bigdata-etl_default runtime-fe-1 >/dev/null 2>&1 || true
docker network connect bigdata-etl_default runtime-be-1 >/dev/null 2>&1 || true
docker run -d --name bigdata-flink-realtime --network bigdata-etl_default \
  -e TZ=Asia/Shanghai \
  -v "${PROJECT_DIR}:/workspace" -w /workspace \
  eclipse-temurin:17-jre \
  java \
  -Duser.timezone=Asia/Shanghai \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  -cp bigdata-etl-pipeline/target/bigdata-etl-pipeline-1.0.0-SNAPSHOT.jar \
  com.bigdata.etl.pipeline.job.UserBehaviorRealtimeJob \
  --kafka.bootstrap.servers=kafka:29092 \
  --kafka.group.id="${FLINK_GROUP_ID}" \
  --flink.parallelism=2 \
  --checkpoint.enabled=false \
  --doris.detail.load-url=http://runtime-be-1:8040/api/bigdata_analytics/dwd_event_detail/_stream_load \
  --doris.aggregate.load-url=http://runtime-be-1:8040/api/bigdata_analytics/dws_event_1m/_stream_load \
  --doris.detail.batch-size=1000 \
  --doris.detail.flush-ms=1000 \
  --doris.aggregate.batch-size=200 \
  --doris.aggregate.flush-ms=1000 >/dev/null
sleep 10

echo "[5/7] Sending realistic behavior events through Nginx Lua"
now_s="$(date +%s)"
base_s="$(((now_s - 60) / 60 * 60))"
sent=0

for i in $(seq 1 "${USERS}"); do
  user_id="$(printf "demo_user_%04d" "${i}")"
  device_id="$(printf "demo_device_%04d" "${i}")"
  session_id="demo_session_${base_s}_${i}"
  product_id="$(printf "sku_%03d" $(((i % 12) + 1)))"
  amount=$((99 + (i % 8) * 20))
  offset=$((i % 50))

  send_event "seed_${base_s}_${i}_page_view" "${user_id}" "${device_id}" "${session_id}" "page_view" "$(((base_s + offset) * 1000))" "/home" "${product_id}" 0
  sent=$((sent + 1))

  if [ "${i}" -le $((USERS * 85 / 100)) ]; then
    send_event "seed_${base_s}_${i}_product_view" "${user_id}" "${device_id}" "${session_id}" "product_view" "$(((base_s + offset + 5) * 1000))" "/product/${product_id}" "${product_id}" 0
    sent=$((sent + 1))
  fi

  if [ "${i}" -le $((USERS * 60 / 100)) ]; then
    send_event "seed_${base_s}_${i}_add_cart" "${user_id}" "${device_id}" "${session_id}" "add_cart" "$(((base_s + offset + 10) * 1000))" "/cart" "${product_id}" "${amount}"
    sent=$((sent + 1))
  fi

  if [ "${i}" -le $((USERS * 38 / 100)) ]; then
    send_event "seed_${base_s}_${i}_checkout" "${user_id}" "${device_id}" "${session_id}" "checkout" "$(((base_s + offset + 15) * 1000))" "/checkout" "${product_id}" "${amount}"
    sent=$((sent + 1))
  fi

  if [ "${i}" -le $((USERS * 28 / 100)) ]; then
    send_event "seed_${base_s}_${i}_pay_success" "${user_id}" "${device_id}" "${session_id}" "pay_success" "$(((base_s + offset + 20) * 1000))" "/pay/success" "${product_id}" "${amount}"
    sent=$((sent + 1))
  fi
done

watermark_ms="$((now_s * 1000))"
send_event "seed_${base_s}_watermark_advance" "demo_watermark_user" "demo_watermark_device" "demo_watermark_session_${now_s}" "page_view" "${watermark_ms}" "/watermark" "sku_000" 0
sent=$((sent + 1))
echo "[ok] Sent ${sent} events to ${INGEST_URL}"

echo "[6/7] Waiting for Flink to write Doris DWD/DWS"
for _ in $(seq 1 60); do
  dwd_count="$(mysql_cmd -N -B -e "use bigdata_analytics; select count(*) from dwd_event_detail;" | tr -d '[:space:]')"
  dws_count="$(mysql_cmd -N -B -e "use bigdata_analytics; select count(*) from dws_event_1m;" | tr -d '[:space:]')"
  if [ "${dwd_count}" -ge "${sent}" ] && [ "${dws_count}" -gt 0 ]; then
    break
  fi
  sleep 2
done

echo "[7/7] Doris and frontend verification"
mysql_cmd <<SQL
use bigdata_analytics;
select count(*) as dwd_event_detail_count from dwd_event_detail;
select count(*) as dws_event_1m_count from dws_event_1m;
select event_name, sum(pv) pv, sum(uv) uv, sum(users) users
from dws_event_1m
group by event_name
order by pv desc;
SQL

echo "[frontend] ${FRONTEND_API}"
curl -fsS --max-time 10 "${FRONTEND_API}"
echo
