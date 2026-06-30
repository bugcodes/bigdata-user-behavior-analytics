package com.bigdata.backend.dto;

/**
 * 行为事件指标，负责承载事件级 UV、PV、用户数与延迟数据。
 *
 * @author zhaobinjie
 * @date 2026-06-25
 */
public record EventMetricDto(String eventName, long pv, long uv, long users, double avgLatencyMs) {
}
