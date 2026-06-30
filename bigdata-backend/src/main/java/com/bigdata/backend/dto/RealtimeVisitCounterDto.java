package com.bigdata.backend.dto;

/**
 * 首页访问实时计数结果，负责承载旁路秒级 PV/UV 指标。
 *
 * @author zhaobinjie
 * @date 2026-06-30
 */
public record RealtimeVisitCounterDto(long pv, long uv) {
}
