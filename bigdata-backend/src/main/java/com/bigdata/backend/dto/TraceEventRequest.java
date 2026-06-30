package com.bigdata.backend.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.Map;

/**
 * 埋点模拟请求，负责接收控制台触发的测试事件。
 *
 * @author zhaobinjie
 * @date 2026-06-25
 */
public record TraceEventRequest(
        @NotBlank String appId,
        @NotBlank String userId,
        @NotBlank String deviceId,
        @NotBlank String sessionId,
        @NotBlank String eventName,
        @NotNull Long eventTime,
        Map<String, Object> properties) {
}
