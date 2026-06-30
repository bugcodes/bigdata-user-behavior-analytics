package com.bigdata.etl.common.util;

import com.bigdata.etl.common.model.CleanEvent;

/**
 * 事件 Key 工具，负责生成 Flink 分区、幂等写入和去重使用的业务键。
 *
 * @author zhaobinjie
 * @date 2026-06-25
 */
public final class EventKeyUtil {

    private EventKeyUtil() {
    }

    public static String eventKey(CleanEvent event) {
        return event.appId() + "|" + event.eventName();
    }

    public static String idempotentKey(CleanEvent event) {
        return event.appId() + "|" + event.eventId();
    }
}
