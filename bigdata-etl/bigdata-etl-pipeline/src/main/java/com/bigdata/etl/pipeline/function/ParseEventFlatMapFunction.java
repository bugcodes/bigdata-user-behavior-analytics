package com.bigdata.etl.pipeline.function;

import com.bigdata.etl.common.model.BehaviorEvent;
import com.bigdata.etl.common.serde.JsonSerde;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 原始事件解析函数，负责把 Kafka JSON 字符串转换成行为事件并过滤坏数据。
 *
 * @author zhaobinjie
 * @date 2026-06-25
 */
public class ParseEventFlatMapFunction implements FlatMapFunction<String, BehaviorEvent> {

    private static final Logger log = LoggerFactory.getLogger(ParseEventFlatMapFunction.class);

    @Override
    public void flatMap(String value, Collector<BehaviorEvent> out) {
        try {
            BehaviorEvent event = JsonSerde.fromJson(value, BehaviorEvent.class);
            if (event.appId() != null && event.eventName() != null && event.eventTime() > 0) {
                out.collect(event);
            }
        } catch (Exception e) {
            log.warn("discard invalid behavior event: {}", value, e);
        }
    }
}
