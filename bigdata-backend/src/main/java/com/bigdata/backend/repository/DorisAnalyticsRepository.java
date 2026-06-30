package com.bigdata.backend.repository;

import com.bigdata.backend.dto.EventMetricDto;
import com.bigdata.backend.dto.FunnelStepDto;
import com.bigdata.backend.dto.TrendPointDto;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Doris 查询仓储，负责从实时明细表和聚合表读取看板指标。
 *
 * @author zhaobinjie
 * @date 2026-06-25
 */
@Repository
@ConditionalOnProperty(name = "analytics.mock-enabled", havingValue = "false")
@ConditionalOnExpression("'${analytics.file-enabled:false}' == 'false'")
public class DorisAnalyticsRepository implements MetricsRepository {

    private final JdbcTemplate jdbcTemplate;
    private final String appId;

    public DorisAnalyticsRepository(JdbcTemplate jdbcTemplate, @Value("${analytics.app-id}") String appId) {
        this.jdbcTemplate = jdbcTemplate;
        this.appId = appId;
    }

    @Override
    public long todayPv() {
        return queryLong("select coalesce(sum(pv),0) from dws_event_1m where app_id=? and window_start >= curdate()", appId);
    }

    @Override
    public long todayUv() {
        return queryLong("select coalesce(ndv(user_id),0) from dwd_event_detail where app_id=? and event_date=curdate()", appId);
    }

    @Override
    public long todayDau() {
        return queryLong("select coalesce(ndv(user_id),0) from dwd_event_detail where app_id=? and event_date=curdate()", appId);
    }

    @Override
    public double ingestTps() {
        Number value = jdbcTemplate.queryForObject("""
                select coalesce(sum(pv) / 60, 0)
                from dws_event_1m
                where app_id=? and window_start >= date_sub(now(), interval 1 minute)
                """, Number.class, appId);
        return value == null ? 0 : value.doubleValue();
    }

    @Override
    public double p99LatencyMs() {
        Number value = jdbcTemplate.queryForObject("""
                select coalesce(percentile_approx(process_latency_ms, 0.99), 0)
                from dwd_event_detail
                where app_id=? and event_date=curdate()
                """, Number.class, appId);
        return value == null ? 0 : value.doubleValue();
    }

    @Override
    public double errorRate() {
        Number value = jdbcTemplate.queryForObject("""
                select coalesce(sum(if(valid=0,1,0)) / greatest(count(*),1), 0)
                from dwd_event_detail
                where app_id=? and event_date=curdate()
                """, Number.class, appId);
        return value == null ? 0 : value.doubleValue();
    }

    @Override
    public List<TrendPointDto> trend() {
        return jdbcTemplate.query("""
                select date_format(window_start, '%H:%i') minute, sum(pv) pv, sum(uv) uv, sum(pv) / 60 tps
                from dws_event_1m
                where app_id=? and window_start >= date_sub(now(), interval 30 minute)
                group by minute
                order by minute
                """, (rs, rowNum) -> new TrendPointDto(
                rs.getString("minute"),
                rs.getLong("pv"),
                rs.getLong("uv"),
                rs.getDouble("tps")), appId);
    }

    @Override
    public List<EventMetricDto> topEvents() {
        return jdbcTemplate.query("""
                select event_name, sum(pv) pv, sum(uv) uv, sum(users) users, avg(avg_latency_ms) avg_latency_ms
                from dws_event_1m
                where app_id=? and window_start >= date_sub(now(), interval 30 minute)
                group by event_name
                order by pv desc
                limit 10
                """, (rs, rowNum) -> new EventMetricDto(
                rs.getString("event_name"),
                rs.getLong("pv"),
                rs.getLong("uv"),
                rs.getLong("users"),
                rs.getDouble("avg_latency_ms")), appId);
    }

    @Override
    public List<FunnelStepDto> funnel() {
        List<FunnelStepDto> funnel = jdbcTemplate.query("""
                select step_name, users, conversion_rate
                from ads_checkout_funnel
                where app_id=? and stat_date=curdate()
                order by step_order
                """, (rs, rowNum) -> new FunnelStepDto(
                rs.getString("step_name"),
                rs.getLong("users"),
                rs.getDouble("conversion_rate")), appId);
        if (!funnel.isEmpty()) {
            return funnel;
        }
        return realtimeFunnelFromDwd();
    }

    private List<FunnelStepDto> realtimeFunnelFromDwd() {
        Map<String, Long> usersByEvent = new HashMap<>();
        RowCallbackHandler handler = rs -> usersByEvent.put(rs.getString("event_name"), rs.getLong("users"));
        jdbcTemplate.query("""
                select event_name, ndv(user_id) users
                from dwd_event_detail
                where app_id=? and event_date=curdate()
                  and event_name in ('product_view','add_cart','checkout','pay_success')
                group by event_name
                """, handler, appId);
        long productView = usersByEvent.getOrDefault("product_view", 0L);
        long base = Math.max(productView, 1L);
        List<FunnelStepDto> result = new ArrayList<>();
        result.add(new FunnelStepDto("访问商品", productView, productView / (double) base));
        result.add(new FunnelStepDto("加入购物车", usersByEvent.getOrDefault("add_cart", 0L), usersByEvent.getOrDefault("add_cart", 0L) / (double) base));
        result.add(new FunnelStepDto("提交订单", usersByEvent.getOrDefault("checkout", 0L), usersByEvent.getOrDefault("checkout", 0L) / (double) base));
        result.add(new FunnelStepDto("支付成功", usersByEvent.getOrDefault("pay_success", 0L), usersByEvent.getOrDefault("pay_success", 0L) / (double) base));
        return result;
    }

    private long queryLong(String sql, Object... args) {
        Number value = jdbcTemplate.queryForObject(sql, Number.class, args);
        return value == null ? 0 : value.longValue();
    }
}
