package com.bigdata.backend.dto;

/**
 * 分钟级趋势点，负责表示实时曲线中的时间、PV、UV 与吞吐。
 *
 * @author zhaobinjie
 * @date 2026-06-25
 */
public record TrendPointDto(String minute, long pv, long uv, double tps) {
}
