package com.bigdata.backend.controller;

import com.bigdata.backend.common.ApiResponse;
import com.bigdata.backend.dto.RealtimeSummaryDto;
import com.bigdata.backend.dto.SeedJobRequest;
import com.bigdata.backend.dto.SeedJobSnapshotDto;
import com.bigdata.backend.dto.TraceEventRequest;
import com.bigdata.backend.service.AnalyticsService;
import com.bigdata.backend.service.SeedJobService;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 分析看板接口，负责向前端提供实时指标与埋点模拟能力。
 *
 * @author zhaobinjie
 * @date 2026-06-25
 */
@RestController
@RequestMapping("/api/analytics")
public class AnalyticsController {

    private final AnalyticsService analyticsService;
    private final SeedJobService seedJobService;

    public AnalyticsController(AnalyticsService analyticsService, SeedJobService seedJobService) {
        this.analyticsService = analyticsService;
        this.seedJobService = seedJobService;
    }

    @GetMapping("/realtime")
    public ApiResponse<RealtimeSummaryDto> realtime() {
        return ApiResponse.success(analyticsService.realtimeSummary());
    }

    @PostMapping("/trace")
    public ApiResponse<String> trace(@Valid @RequestBody TraceEventRequest request) {
        return ApiResponse.success(analyticsService.sendTraceEvent(request));
    }

    @PostMapping("/seed/start")
    public ApiResponse<SeedJobSnapshotDto> startSeedJob(@Valid @RequestBody SeedJobRequest request) {
        return ApiResponse.success(seedJobService.start(request));
    }

    @GetMapping("/seed/status/{jobId}")
    public ApiResponse<SeedJobSnapshotDto> seedJobStatus(@PathVariable String jobId) {
        return ApiResponse.success(seedJobService.status(jobId));
    }

    @PostMapping("/seed/cancel/{jobId}")
    public ApiResponse<SeedJobSnapshotDto> cancelSeedJob(@PathVariable String jobId) {
        return ApiResponse.success(seedJobService.cancel(jobId));
    }
}
