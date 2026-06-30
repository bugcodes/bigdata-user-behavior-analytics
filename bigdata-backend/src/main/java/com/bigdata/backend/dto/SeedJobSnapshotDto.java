package com.bigdata.backend.dto;

/**
 * 批量造数任务快照，负责向前端呈现生成、发送、入库和聚合进度。
 *
 * @author zhaobinjie
 * @date 2026-06-25
 */
public record SeedJobSnapshotDto(
        String jobId,
        String status,
        int users,
        long expectedEvents,
        long sentEvents,
        long failedEvents,
        long dwdCount,
        long dwsCount,
        long dwdDelta,
        long dwsDelta,
        long dorisLag,
        double sendTps,
        double progress,
        String currentStage,
        String message,
        long startedAt,
        long updatedAt,
        long finishedAt) {
}
