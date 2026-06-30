package com.bigdata.backend.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

/**
 * 批量造数任务请求，负责描述需要模拟的用户规模与发送节奏。
 *
 * @author zhaobinjie
 * @date 2026-06-25
 */
public record SeedJobRequest(
        @Min(1) @Max(5_000_000) int users,
        @Min(1) @Max(20_000) int batchSize,
        @Min(0) @Max(10_000) int pauseMs) {
}
