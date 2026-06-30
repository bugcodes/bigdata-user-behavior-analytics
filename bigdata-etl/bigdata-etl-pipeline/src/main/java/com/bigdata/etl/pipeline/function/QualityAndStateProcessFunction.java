package com.bigdata.etl.pipeline.function;

import com.bigdata.etl.common.model.BehaviorEvent;
import com.bigdata.etl.common.model.CleanEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

/**
 * 数据质量与状态处理函数，负责事件校验、状态 TTL 去重和处理延迟计算。
 *
 * @author zhaobinjie
 * @date 2026-06-25
 */
public class QualityAndStateProcessFunction extends KeyedProcessFunction<String, BehaviorEvent, CleanEvent> {

    private static final ZoneId BUSINESS_ZONE = ZoneId.of("Asia/Shanghai");

    private transient ValueState<Boolean> seenState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("event-seen", Types.BOOLEAN);
        descriptor.enableTimeToLive(org.apache.flink.api.common.state.StateTtlConfig
                .newBuilder(Time.days(1))
                .setUpdateType(org.apache.flink.api.common.state.StateTtlConfig.UpdateType.OnCreateAndWrite)
                .cleanupInRocksdbCompactFilter(1000)
                .build());
        seenState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(BehaviorEvent value, Context ctx, Collector<CleanEvent> out) throws Exception {
        if (seenState.value() != null) {
            return;
        }
        seenState.update(true);
        long processTime = System.currentTimeMillis();
        boolean valid = value.userId() != null && value.deviceId() != null && value.eventName() != null;
        LocalDate eventDate = Instant.ofEpochMilli(value.eventTime()).atZone(BUSINESS_ZONE).toLocalDate();
        out.collect(new CleanEvent(
                value.eventId(),
                value.appId(),
                value.userId(),
                value.deviceId(),
                value.sessionId(),
                value.eventName(),
                value.eventTime(),
                value.receiveTime(),
                processTime,
                Math.max(0, processTime - value.receiveTime()),
                valid,
                eventDate,
                value.ip(),
                value.userAgent(),
                value.properties()));
    }
}
