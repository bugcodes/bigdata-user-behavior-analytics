package com.bigdata.etl.pipeline.function;

import com.bigdata.etl.common.model.EventAccumulator;
import com.bigdata.etl.common.model.EventAggregate;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * 事件窗口补全函数，负责把窗口边界与业务 Key 写入聚合结果。
 *
 * @author zhaobinjie
 * @date 2026-06-25
 */
public class EventWindowFunction extends ProcessWindowFunction<EventAccumulator, EventAggregate, String, org.apache.flink.streaming.api.windowing.windows.TimeWindow> {

    private static final ZoneId BUSINESS_ZONE = ZoneId.of("Asia/Shanghai");

    @Override
    public void process(String key, Context context, Iterable<EventAccumulator> elements, Collector<EventAggregate> out) {
        String[] parts = key.split("\\|", 2);
        EventAccumulator accumulator = elements.iterator().next();
        out.collect(new EventAggregate(
                parts[0],
                parts.length > 1 ? parts[1] : "unknown",
                LocalDateTime.ofInstant(Instant.ofEpochMilli(context.window().getStart()), BUSINESS_ZONE),
                LocalDateTime.ofInstant(Instant.ofEpochMilli(context.window().getEnd()), BUSINESS_ZONE),
                accumulator.pv(),
                accumulator.uv(),
                accumulator.users(),
                accumulator.avgLatencyMs()));
    }
}
