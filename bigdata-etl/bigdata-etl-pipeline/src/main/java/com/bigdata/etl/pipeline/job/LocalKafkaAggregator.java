package com.bigdata.etl.pipeline.job;

import com.bigdata.etl.common.model.EventAggregate;
import com.bigdata.etl.common.serde.JsonSerde;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * 本地 Kafka 聚合器，负责在 Flink/Doris 启动前把真实 Kafka 埋点聚合成看板文件。
 *
 * @author zhaobinjie
 * @date 2026-06-25
 */
public class LocalKafkaAggregator {

    public static void main(String[] args) throws Exception {
        Properties config = UserBehaviorRealtimeJob.JobConfig.load(args);
        String output = config.getProperty("local.aggregate.path", "runtime/realtime/event_aggregate.jsonl");
        Path outputPath = Path.of(output);
        Files.createDirectories(outputPath.getParent());

        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getProperty("kafka.bootstrap.servers", "127.0.0.1:9092"));
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, config.getProperty("kafka.local.group.id", "bigdata-local-kafka-aggregator"));
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        Map<String, MutableAggregate> aggregates = new HashMap<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
             BufferedWriter writer = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8,
                     StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            consumer.subscribe(List.of(config.getProperty("kafka.topic.raw", "behavior_raw")));
            while (true) {
                consumer.poll(Duration.ofMillis(1000)).forEach(record -> {
                    try {
                        JsonNode event = JsonSerde.fromJson(record.value(), JsonNode.class);
                        String appId = event.path("appId").asText("demo-app");
                        String eventName = event.path("eventName").asText("unknown");
                        String key = appId + "|" + eventName;
                        MutableAggregate aggregate = aggregates.computeIfAbsent(key, ignored -> new MutableAggregate(appId, eventName));
                        aggregate.add(event.path("deviceId").asText(""), event.path("userId").asText(""),
                                Math.max(0, System.currentTimeMillis() - event.path("receiveTime").asLong(System.currentTimeMillis())));
                    } catch (Exception ignored) {
                        // Ignore malformed demo messages and keep the local stream alive.
                    }
                });
                writer.flush();
                Files.writeString(outputPath, "", StandardOpenOption.TRUNCATE_EXISTING);
                try (BufferedWriter snapshot = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8,
                        StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                    for (MutableAggregate aggregate : aggregates.values()) {
                        snapshot.write(JsonSerde.toJson(aggregate.toRow()));
                        snapshot.newLine();
                    }
                }
            }
        }
    }

    private static final class MutableAggregate {
        private final String appId;
        private final String eventName;
        private final Set<String> devices = new HashSet<>();
        private final Set<String> users = new HashSet<>();
        private long pv;
        private long latencyMs;

        private MutableAggregate(String appId, String eventName) {
            this.appId = appId;
            this.eventName = eventName;
        }

        private void add(String deviceId, String userId, long latency) {
            pv++;
            devices.add(deviceId);
            users.add(userId);
            latencyMs += latency;
        }

        private EventAggregate toRow() {
            LocalDateTime now = LocalDateTime.now().withNano(0);
            return new EventAggregate(appId, eventName, now, now.plusSeconds(10), pv, devices.size(), users.size(),
                    pv == 0 ? 0 : latencyMs / (double) pv);
        }
    }
}
