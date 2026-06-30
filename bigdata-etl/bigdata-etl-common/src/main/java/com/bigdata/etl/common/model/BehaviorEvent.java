package com.bigdata.etl.common.model;

import java.io.Serializable;
import java.util.Map;

/**
 * 原始行为事件，负责承载 Nginx Lua 写入 Kafka 的埋点上报内容。
 *
 * @author zhaobinjie
 * @date 2026-06-25
 */
public record BehaviorEvent(
        String eventId,
        String appId,
        String userId,
        String deviceId,
        String sessionId,
        String eventName,
        long eventTime,
        long receiveTime,
        String ip,
        String userAgent,
        Map<String, Object> properties) implements Serializable {
}
