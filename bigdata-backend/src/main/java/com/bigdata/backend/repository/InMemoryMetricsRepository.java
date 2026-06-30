package com.bigdata.backend.repository;

import com.bigdata.backend.dto.EventMetricDto;
import com.bigdata.backend.dto.FunnelStepDto;
import com.bigdata.backend.dto.TrendPointDto;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Repository;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.IntStream;

/**
 * 本地指标仓储，负责在 Doris 未启动时提供可演示的实时看板数据。
 *
 * @author zhaobinjie
 * @date 2026-06-25
 */
@Repository
@ConditionalOnProperty(name = "analytics.mock-enabled", havingValue = "true", matchIfMissing = true)
public class InMemoryMetricsRepository implements MetricsRepository {

    @Override
    public long todayPv() {
        return 1_284_593_120L;
    }

    @Override
    public long todayUv() {
        return 28_640_913L;
    }

    @Override
    public long todayDau() {
        return 18_923_410L;
    }

    @Override
    public double ingestTps() {
        return 12_280.6;
    }

    @Override
    public double p99LatencyMs() {
        return 812.4;
    }

    @Override
    public double errorRate() {
        return 0.0017;
    }

    @Override
    public List<TrendPointDto> trend() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm");
        return IntStream.range(0, 30)
                .mapToObj(i -> new TrendPointDto(
                        LocalTime.now().minusMinutes(29L - i).format(formatter),
                        520_000L + i * 13_700L,
                        83_000L + i * 1_910L,
                        8_700 + i * 219.5))
                .toList();
    }

    @Override
    public List<EventMetricDto> topEvents() {
        return List.of(
                new EventMetricDto("page_view", 482_120_000L, 21_802_000L, 18_006_000L, 62.4),
                new EventMetricDto("product_view", 186_900_000L, 12_381_000L, 9_730_000L, 74.6),
                new EventMetricDto("add_cart", 68_260_000L, 5_908_000L, 4_420_000L, 83.2),
                new EventMetricDto("checkout", 24_380_000L, 2_190_000L, 1_860_000L, 96.1),
                new EventMetricDto("pay_success", 18_720_000L, 1_740_000L, 1_590_000L, 102.8));
    }

    @Override
    public List<FunnelStepDto> funnel() {
        return List.of(
                new FunnelStepDto("访问商品", 12_381_000L, 1.0),
                new FunnelStepDto("加入购物车", 5_908_000L, 0.477),
                new FunnelStepDto("提交订单", 2_190_000L, 0.177),
                new FunnelStepDto("支付成功", 1_740_000L, 0.141));
    }
}
