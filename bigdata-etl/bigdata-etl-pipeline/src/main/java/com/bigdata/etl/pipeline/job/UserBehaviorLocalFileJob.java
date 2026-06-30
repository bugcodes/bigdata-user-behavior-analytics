package com.bigdata.etl.pipeline.job;

import com.bigdata.etl.common.model.CleanEvent;
import com.bigdata.etl.common.model.EventAggregate;
import com.bigdata.etl.common.util.EventKeyUtil;
import com.bigdata.etl.pipeline.function.EventAggregateFunction;
import com.bigdata.etl.pipeline.function.EventWindowFunction;
import com.bigdata.etl.pipeline.function.ParseEventFlatMapFunction;
import com.bigdata.etl.pipeline.function.QualityAndStateProcessFunction;
import com.bigdata.etl.pipeline.sink.JsonLineFileSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.util.Properties;

/**
 * 本地文件实时计算作业，负责在 Doris 未就绪时用 Flink 生成真实看板指标。
 *
 * @author zhaobinjie
 * @date 2026-06-25
 */
public class UserBehaviorLocalFileJob {

    public static void main(String[] args) throws Exception {
        Properties properties = UserBehaviorRealtimeJob.JobConfig.load(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureRuntime(env, properties);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(properties.getProperty("kafka.bootstrap.servers", "127.0.0.1:9092"))
                .setTopics(properties.getProperty("kafka.topic.raw", "behavior_raw"))
                .setGroupId(properties.getProperty("kafka.local.group.id", "bigdata-etl-local-file"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        SingleOutputStreamOperator<CleanEvent> cleanEvents = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-raw-source")
                .flatMap(new ParseEventFlatMapFunction())
                .keyBy(event -> event.appId() + "|" + event.eventId())
                .process(new QualityAndStateProcessFunction())
                .name("quality-state-dedup");

        SingleOutputStreamOperator<EventAggregate> aggregates = cleanEvents
                .keyBy(EventKeyUtil::eventKey)
                .window(org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(
                        Long.parseLong(properties.getProperty("local.window.seconds", "10")))))
                .aggregate(new EventAggregateFunction(), new EventWindowFunction())
                .name("event-local-aggregate");

        aggregates.addSink(new JsonLineFileSink<>(
                        properties.getProperty("local.aggregate.path", "runtime/realtime/event_aggregate.jsonl")))
                .name("local-jsonl-aggregate");

        env.execute("bigdata-user-behavior-local-file");
    }

    private static void configureRuntime(StreamExecutionEnvironment env, Properties properties) {
        env.setParallelism(Integer.parseInt(properties.getProperty("flink.parallelism", "2")));
        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(Long.parseLong(properties.getProperty("checkpoint.interval-ms", "60000")), CheckpointingMode.AT_LEAST_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));
    }
}
