package com.bigdata.backend.dto;

/**
 * 漏斗步骤指标，负责描述转化链路中每一步的规模与转化率。
 *
 * @author zhaobinjie
 * @date 2026-06-25
 */
public record FunnelStepDto(String step, long users, double conversionRate) {
}
