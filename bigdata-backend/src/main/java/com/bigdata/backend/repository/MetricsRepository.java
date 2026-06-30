package com.bigdata.backend.repository;

import com.bigdata.backend.dto.EventMetricDto;
import com.bigdata.backend.dto.FunnelStepDto;
import com.bigdata.backend.dto.TrendPointDto;

import java.util.List;

/**
 * 指标查询接口，负责屏蔽 Doris 查询与本地演示数据的差异。
 *
 * @author zhaobinjie
 * @date 2026-06-25
 */
public interface MetricsRepository {

    long todayPv();

    long todayUv();

    long todayDau();

    double ingestTps();

    double p99LatencyMs();

    double errorRate();

    List<TrendPointDto> trend();

    List<EventMetricDto> topEvents();

    List<FunnelStepDto> funnel();
}
