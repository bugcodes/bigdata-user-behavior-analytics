package com.bigdata.etl.pipeline.job;

import com.bigdata.etl.common.model.BehaviorEvent;
import com.bigdata.etl.common.model.CleanEvent;
import com.bigdata.etl.common.model.EventAccumulator;
import com.bigdata.etl.common.model.EventAggregate;
import com.bigdata.etl.common.serde.JsonSerde;
import com.bigdata.etl.common.util.EventKeyUtil;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.bigdata.etl.pipeline.function.EventAggregateFunction;
import com.bigdata.etl.pipeline.function.EventWindowFunction;
import com.bigdata.etl.pipeline.function.ParseEventFlatMapFunction;
import com.bigdata.etl.pipeline.function.QualityAndStateProcessFunction;
import com.bigdata.etl.pipeline.sink.DorisStreamLoadSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Properties;

/**
 * 用户行为实时计算作业，负责完成 Kafka 接入、Watermark、状态去重、窗口聚合和 Doris 写入。
 *
 * @author zhaobinjie
 * @date 2026-06-25
 */
public class UserBehaviorRealtimeJob {

    private static final OutputTag<CleanEvent> LATE_EVENT_TAG = new OutputTag<>("late-events") {
    };

    public static void main(String[] args) throws Exception {
        Properties properties = JobConfig.load(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureRuntime(env, properties);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(properties.getProperty("kafka.bootstrap.servers", "127.0.0.1:9092"))
                .setTopics(properties.getProperty("kafka.topic.raw", "behavior_raw"))
                .setGroupId(properties.getProperty("kafka.group.id", "bigdata-etl-realtime"))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        SingleOutputStreamOperator<CleanEvent> cleanEvents = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-raw-source")
                .flatMap(new ParseEventFlatMapFunction())
                .name("parse-json")
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<BehaviorEvent>forBoundedOutOfOrderness(Duration.ofSeconds(
                                Long.parseLong(properties.getProperty("watermark.max-out-of-orderness.seconds", "10"))))
                        .withIdleness(Duration.ofSeconds(Long.parseLong(properties.getProperty("watermark.idle-timeout.seconds", "10"))))
                        .withTimestampAssigner((event, timestamp) -> event.eventTime()))
                .keyBy(event -> event.appId() + "|" + event.eventId())
                .process(new QualityAndStateProcessFunction())
                .name("quality-state-dedup");

        SingleOutputStreamOperator<EventAggregate> aggregates = cleanEvents
                .keyBy(EventKeyUtil::eventKey)
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.minutes(1)))
                .allowedLateness(org.apache.flink.streaming.api.windowing.time.Time.minutes(5))
                .sideOutputLateData(LATE_EVENT_TAG)
                .aggregate(new EventAggregateFunction(), new EventWindowFunction())
                .name("event-1m-aggregate");

        cleanEvents.addSink(new DorisStreamLoadSink<>(
                properties.getProperty("doris.detail.load-url", "http://127.0.0.1:8030/api/bigdata_analytics/dwd_event_detail/_stream_load"),
                properties.getProperty("doris.user", "root"),
                properties.getProperty("doris.password", ""),
                Integer.parseInt(properties.getProperty("doris.detail.batch-size", "5000")),
                Long.parseLong(properties.getProperty("doris.detail.flush-ms", "3000"))))
                .name("doris-dwd-detail");

        aggregates.addSink(new DorisStreamLoadSink<>(
                properties.getProperty("doris.aggregate.load-url", "http://127.0.0.1:8030/api/bigdata_analytics/dws_event_1m/_stream_load"),
                properties.getProperty("doris.user", "root"),
                properties.getProperty("doris.password", ""),
                Integer.parseInt(properties.getProperty("doris.aggregate.batch-size", "1000")),
                Long.parseLong(properties.getProperty("doris.aggregate.flush-ms", "2000"))))
                .name("doris-dws-event-1m");

        DataStream<String> lateEvents = aggregates.getSideOutput(LATE_EVENT_TAG)
                .map(JsonSerde::toJson)
                .name("late-event-json");
        lateEvents.sinkTo(KafkaSink.<String>builder()
                .setBootstrapServers(properties.getProperty("kafka.bootstrap.servers", "127.0.0.1:9092"))
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(properties.getProperty("kafka.topic.late", "behavior_late"))
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build())
                .name("late-event-kafka-sink");

        env.execute("bigdata-user-behavior-realtime");
    }

    private static void configureRuntime(StreamExecutionEnvironment env, Properties properties) {
        env.setParallelism(Integer.parseInt(properties.getProperty("flink.parallelism", "4")));
        env.setStateBackend(new HashMapStateBackend());
        env.getConfig().registerTypeWithKryoSerializer(BehaviorEvent.class, JavaSerializer.class);
        env.getConfig().registerTypeWithKryoSerializer(CleanEvent.class, JavaSerializer.class);
        env.getConfig().registerTypeWithKryoSerializer(EventAggregate.class, JavaSerializer.class);
        env.getConfig().registerTypeWithKryoSerializer(EventAccumulator.class, JavaSerializer.class);
        boolean checkpointEnabled = Boolean.parseBoolean(properties.getProperty("checkpoint.enabled", "true"));
        if (checkpointEnabled) {
            env.enableCheckpointing(Long.parseLong(properties.getProperty("checkpoint.interval-ms", "60000")), CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(Long.parseLong(properties.getProperty("checkpoint.min-pause-ms", "30000")));
            env.getCheckpointConfig().setCheckpointTimeout(Long.parseLong(properties.getProperty("checkpoint.timeout-ms", "120000")));
            env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        }
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));
    }

    /**
     * 作业配置加载器，负责合并 classpath 配置文件与命令行覆盖参数。
     *
     * @author zhaobinjie
     * @date 2026-06-25
     */
    public static class JobConfig {

        static Properties load(String[] args) throws Exception {
            Properties properties = new Properties();
            try (var input = UserBehaviorRealtimeJob.class.getClassLoader().getResourceAsStream("application.properties")) {
                if (input != null) {
                    properties.load(input);
                }
            }
            for (String arg : args) {
                if (arg.startsWith("--") && arg.contains("=")) {
                    String[] parts = arg.substring(2).split("=", 2);
                    properties.setProperty(parts[0], parts[1]);
                }
            }
            return properties;
        }
    }
}
