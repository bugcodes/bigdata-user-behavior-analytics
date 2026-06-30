package com.bigdata.backend.repository;

import com.bigdata.backend.dto.EventMetricDto;
import com.bigdata.backend.dto.FunnelStepDto;
import com.bigdata.backend.dto.TrendPointDto;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 本地文件指标仓储，负责读取 Flink 本地 JSONL 聚合结果生成真实看板数据。
 *
 * @author zhaobinjie
 * @date 2026-06-25
 */
@Repository
@ConditionalOnProperty(name = "analytics.file-enabled", havingValue = "true")
public class FileMetricsRepository implements MetricsRepository {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private final Path aggregatePath;

    public FileMetricsRepository(@Value("${analytics.file.aggregate-path:../bigdata-etl/runtime/realtime/event_aggregate.jsonl}") String aggregatePath) {
        this.aggregatePath = Path.of(aggregatePath);
    }

    @Override
    public long todayPv() {
        return rows().stream().mapToLong(AggregateRow::pv).sum();
    }

    @Override
    public long todayUv() {
        return rows().stream().mapToLong(AggregateRow::uv).sum();
    }

    @Override
    public long todayDau() {
        return rows().stream().mapToLong(AggregateRow::users).sum();
    }

    @Override
    public double ingestTps() {
        return rows().stream().mapToLong(AggregateRow::pv).sum() / 60.0;
    }

    @Override
    public double p99LatencyMs() {
        return rows().stream()
                .mapToDouble(AggregateRow::avgLatencyMs)
                .max()
                .orElse(0);
    }

    @Override
    public double errorRate() {
        return 0;
    }

    @Override
    public List<TrendPointDto> trend() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
        return rows().stream()
                .collect(Collectors.groupingBy(row -> row.windowStart().format(formatter)))
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> new TrendPointDto(
                        entry.getKey(),
                        entry.getValue().stream().mapToLong(AggregateRow::pv).sum(),
                        entry.getValue().stream().mapToLong(AggregateRow::uv).sum(),
                        entry.getValue().stream().mapToLong(AggregateRow::pv).sum() / 10.0))
                .toList();
    }

    @Override
    public List<EventMetricDto> topEvents() {
        return rows().stream()
                .collect(Collectors.groupingBy(AggregateRow::eventName))
                .entrySet()
                .stream()
                .map(entry -> new EventMetricDto(
                        entry.getKey(),
                        entry.getValue().stream().mapToLong(AggregateRow::pv).sum(),
                        entry.getValue().stream().mapToLong(AggregateRow::uv).sum(),
                        entry.getValue().stream().mapToLong(AggregateRow::users).sum(),
                        entry.getValue().stream().mapToDouble(AggregateRow::avgLatencyMs).average().orElse(0)))
                .sorted(Comparator.comparingLong(EventMetricDto::pv).reversed())
                .limit(10)
                .toList();
    }

    @Override
    public List<FunnelStepDto> funnel() {
        Map<String, Long> users = topEvents().stream()
                .collect(Collectors.toMap(EventMetricDto::eventName, EventMetricDto::users, Long::sum));
        long productView = users.getOrDefault("product_view", users.getOrDefault("page_view", 0L));
        long addCart = users.getOrDefault("add_cart", 0L);
        long checkout = users.getOrDefault("checkout", 0L);
        long paySuccess = users.getOrDefault("pay_success", 0L);
        long base = Math.max(productView, 1);
        return List.of(
                new FunnelStepDto("访问商品", productView, productView / (double) base),
                new FunnelStepDto("加入购物车", addCart, addCart / (double) base),
                new FunnelStepDto("提交订单", checkout, checkout / (double) base),
                new FunnelStepDto("支付成功", paySuccess, paySuccess / (double) base));
    }

    private List<AggregateRow> rows() {
        if (!Files.exists(aggregatePath)) {
            return List.of();
        }
        try (var lines = Files.lines(aggregatePath)) {
            return lines.filter(line -> !line.isBlank())
                    .map(this::readRow)
                    .toList();
        } catch (IOException e) {
            return List.of();
        }
    }

    private AggregateRow readRow(String line) {
        try {
            return MAPPER.readValue(line, AggregateRow.class);
        } catch (IOException e) {
            throw new IllegalArgumentException("invalid aggregate row: " + line, e);
        }
    }

    record AggregateRow(
            @JsonProperty("app_id") String appId,
            @JsonProperty("event_name") String eventName,
            @JsonProperty("window_start") @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime windowStart,
            @JsonProperty("window_end") @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime windowEnd,
            long pv,
            long uv,
            long users,
            @JsonProperty("avg_latency_ms") double avgLatencyMs) {
    }
}
