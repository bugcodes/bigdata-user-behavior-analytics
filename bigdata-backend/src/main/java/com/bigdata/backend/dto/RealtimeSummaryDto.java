package com.bigdata.backend.dto;

import java.util.List;

/**
 * 实时看板汇总数据，负责聚合核心指标、趋势、Top 事件与漏斗分析结果。
 *
 * @author zhaobinjie
 * @date 2026-06-25
 */
public record RealtimeSummaryDto(
        long pv,
        long uv,
        long fastPv,
        long fastUv,
        long dau,
        double ingestTps,
        double p99LatencyMs,
        double errorRate,
        List<TrendPointDto> trend,
        List<EventMetricDto> topEvents,
        List<FunnelStepDto> funnel) {
}
