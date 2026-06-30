#!/usr/bin/env bash
set -euo pipefail

kafka-topics --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS:-127.0.0.1:9092}" \
  --create --if-not-exists --topic behavior_raw --partitions 96 --replication-factor "${KAFKA_REPLICATION_FACTOR:-1}"

kafka-topics --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS:-127.0.0.1:9092}" \
  --create --if-not-exists --topic behavior_late --partitions 24 --replication-factor "${KAFKA_REPLICATION_FACTOR:-1}"
