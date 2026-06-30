#!/usr/bin/env bash
set -euo pipefail

INGEST_URL="${INGEST_URL:-http://127.0.0.1:8081/collect}"
USERS="${USERS:-20}"
now_s="$(date +%s)"
base_s="$(((now_s - 60) / 60 * 60))"

send_event() {
  local event_id="$1"
  local user_id="$2"
  local device_id="$3"
  local session_id="$4"
  local event_name="$5"
  local event_time_ms="$6"
  local page="$7"

  local payload
  payload="{
    \"eventId\":\"${event_id}\",
    \"appId\":\"demo-app\",
    \"userId\":\"${user_id}\",
    \"deviceId\":\"${device_id}\",
    \"sessionId\":\"${session_id}\",
    \"eventName\":\"${event_name}\",
    \"eventTime\":${event_time_ms},
    \"properties\":{\"page\":\"${page}\",\"channel\":\"demo-seed-only\"}
  }"

  for attempt in $(seq 1 5); do
    if curl -fsS --max-time 10 -X POST "${INGEST_URL}" \
      -H "Content-Type: application/json" \
      -d "${payload}" >/dev/null; then
      return 0
    fi
    sleep "$((attempt * 2))"
  done

  echo "failed to send event: ${event_id}" >&2
  return 1
}

sent=0
for i in $(seq 1 "${USERS}"); do
  user_id="$(printf "append_user_%04d" "${i}")"
  device_id="$(printf "append_device_%04d" "${i}")"
  session_id="append_session_${base_s}_${i}"
  offset=$((i % 45))

  send_event "append_${base_s}_${i}_page_view" "${user_id}" "${device_id}" "${session_id}" "page_view" "$(((base_s + offset) * 1000))" "/home"
  sent=$((sent + 1))

  if [ "${i}" -le $((USERS * 70 / 100)) ]; then
    send_event "append_${base_s}_${i}_product_view" "${user_id}" "${device_id}" "${session_id}" "product_view" "$(((base_s + offset + 4) * 1000))" "/product"
    sent=$((sent + 1))
  fi

  if [ "${i}" -le $((USERS * 45 / 100)) ]; then
    send_event "append_${base_s}_${i}_add_cart" "${user_id}" "${device_id}" "${session_id}" "add_cart" "$(((base_s + offset + 8) * 1000))" "/cart"
    sent=$((sent + 1))
  fi
done

send_event "append_${base_s}_watermark_advance" "append_watermark_user" "append_watermark_device" "append_watermark_session_${now_s}" "page_view" "$((now_s * 1000))" "/watermark"
sent=$((sent + 1))

echo "sent=${sent}"
