package com.bigdata.etl.pipeline.function;

import com.bigdata.etl.common.model.CleanEvent;
import com.bigdata.etl.common.model.EventAccumulator;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * 事件窗口聚合函数，负责在 Flink 窗口中增量累计行为指标。
 *
 * @author zhaobinjie
 * @date 2026-06-25
 */
public class EventAggregateFunction implements AggregateFunction<CleanEvent, EventAccumulator, EventAccumulator> {

    @Override
    public EventAccumulator createAccumulator() {
        return new EventAccumulator();
    }

    @Override
    public EventAccumulator add(CleanEvent value, EventAccumulator accumulator) {
        accumulator.add(value);
        return accumulator;
    }

    @Override
    public EventAccumulator getResult(EventAccumulator accumulator) {
        return accumulator;
    }

    @Override
    public EventAccumulator merge(EventAccumulator a, EventAccumulator b) {
        return a.merge(b);
    }
}
