package com.bigdata.etl.common.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * 分钟级事件聚合结果，负责写入 Doris 的 DWS 实时指标表。
 *
 * @author zhaobinjie
 * @date 2026-06-25
 */
public record EventAggregate(
        @JsonProperty("app_id")
        String appId,
        @JsonProperty("event_name")
        String eventName,
        @JsonProperty("window_start")
        @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
        LocalDateTime windowStart,
        @JsonProperty("window_end")
        @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
        LocalDateTime windowEnd,
        long pv,
        long uv,
        long users,
        @JsonProperty("avg_latency_ms")
        double avgLatencyMs) implements Serializable {
}
