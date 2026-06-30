package com.bigdata.etl.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.Map;

/**
 * 清洗后的行为事件，负责补充质量标记、延迟和事件日期等实时明细字段。
 *
 * @author zhaobinjie
 * @date 2026-06-25
 */
public record CleanEvent(
        @JsonProperty("event_id")
        String eventId,
        @JsonProperty("app_id")
        String appId,
        @JsonProperty("user_id")
        String userId,
        @JsonProperty("device_id")
        String deviceId,
        @JsonProperty("session_id")
        String sessionId,
        @JsonProperty("event_name")
        String eventName,
        @JsonProperty("event_time")
        long eventTime,
        @JsonProperty("receive_time")
        long receiveTime,
        @JsonProperty("process_time")
        long processTime,
        @JsonProperty("process_latency_ms")
        long processLatencyMs,
        boolean valid,
        @JsonProperty("event_date")
        LocalDate eventDate,
        String ip,
        @JsonProperty("user_agent")
        String userAgent,
        Map<String, Object> properties) implements Serializable {
}
