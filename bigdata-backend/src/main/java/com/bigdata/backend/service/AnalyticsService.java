package com.bigdata.backend.service;

import com.bigdata.backend.dto.RealtimeSummaryDto;
import com.bigdata.backend.dto.RealtimeVisitCounterDto;
import com.bigdata.backend.dto.TraceEventRequest;
import com.bigdata.backend.repository.MetricsRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

/**
 * 分析服务，负责组织实时看板查询与测试埋点上报。
 *
 * @author zhaobinjie
 * @date 2026-06-25
 */
@Service
public class AnalyticsService {

    private final MetricsRepository metricsRepository;
    private final RealtimeVisitCounterService realtimeVisitCounterService;
    private final RestClient restClient;
    private final String ingestEndpoint;

    public AnalyticsService(
            MetricsRepository metricsRepository,
            RealtimeVisitCounterService realtimeVisitCounterService,
            RestClient.Builder restClientBuilder,
            @Value("${analytics.ingest-endpoint}") String ingestEndpoint) {
        this.metricsRepository = metricsRepository;
        this.realtimeVisitCounterService = realtimeVisitCounterService;
        this.restClient = restClientBuilder.build();
        this.ingestEndpoint = ingestEndpoint;
    }

    public RealtimeSummaryDto realtimeSummary() {
        RealtimeVisitCounterDto realtimeCounter = realtimeVisitCounterService.snapshot();
        return new RealtimeSummaryDto(
                metricsRepository.todayPv(),
                metricsRepository.todayUv(),
                realtimeCounter.pv(),
                realtimeCounter.uv(),
                metricsRepository.todayDau(),
                metricsRepository.ingestTps(),
                metricsRepository.p99LatencyMs(),
                metricsRepository.errorRate(),
                metricsRepository.trend(),
                metricsRepository.topEvents(),
                metricsRepository.funnel());
    }

    public String sendTraceEvent(TraceEventRequest request) {
        realtimeVisitCounterService.recordIfPortfolioHome(request);
        return restClient.post()
                .uri(ingestEndpoint)
                .contentType(MediaType.APPLICATION_JSON)
                .body(request)
                .retrieve()
                .body(String.class);
    }
}
