package com.bigdata.backend.service;

import com.bigdata.backend.dto.SeedJobRequest;
import com.bigdata.backend.dto.SeedJobSnapshotDto;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 批量造数任务服务，负责异步模拟用户行为并暴露链路处理进度。
 *
 * @author zhaobinjie
 * @date 2026-06-25
 */
@Service
public class SeedJobService {

    private static final int DEFAULT_USERS = 20_000;
    private static final int DEFAULT_BATCH_SIZE = 1_000;
    private static final int DEFAULT_PAUSE_MS = 0;

    private final RestClient restClient;
    private final JdbcTemplate jdbcTemplate;
    private final String ingestEndpoint;
    private final Map<String, SeedJob> jobs = new ConcurrentHashMap<>();
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    public SeedJobService(
            RestClient.Builder restClientBuilder,
            JdbcTemplate jdbcTemplate,
            @Value("${analytics.ingest-endpoint}") String ingestEndpoint) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(Duration.ofSeconds(2));
        requestFactory.setReadTimeout(Duration.ofSeconds(5));
        this.restClient = restClientBuilder.requestFactory(requestFactory).build();
        this.jdbcTemplate = jdbcTemplate;
        this.ingestEndpoint = ingestEndpoint;
    }

    public SeedJobSnapshotDto start(SeedJobRequest request) {
        int users = request.users() <= 0 ? DEFAULT_USERS : request.users();
        int batchSize = request.batchSize() <= 0 ? DEFAULT_BATCH_SIZE : request.batchSize();
        int pauseMs = request.pauseMs() < 0 ? DEFAULT_PAUSE_MS : request.pauseMs();
        jobs.values().forEach(existingJob -> {
            if ("RUNNING".equals(existingJob.status) || "CREATED".equals(existingJob.status)) {
                existingJob.cancelled.set(true);
                existingJob.status = "CANCELLED";
                existingJob.currentStage = "CANCELLED";
                existingJob.message = "已被新的造数任务替换";
                existingJob.finishedAt.compareAndSet(0, System.currentTimeMillis());
            }
        });
        String jobId = UUID.randomUUID().toString();
        SeedJob job = new SeedJob(jobId, users, expectedEvents(users), batchSize, pauseMs);
        jobs.put(jobId, job);
        CompletableFuture.runAsync(() -> runJob(job), executorService);
        return snapshot(job);
    }

    public SeedJobSnapshotDto status(String jobId) {
        SeedJob job = jobs.get(jobId);
        if (job == null) {
            return new SeedJobSnapshotDto(jobId, "NOT_FOUND", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "UNKNOWN", "任务不存在", 0, 0, 0);
        }
        refreshDorisCountsIfDue(job);
        return snapshot(job);
    }

    public SeedJobSnapshotDto cancel(String jobId) {
        SeedJob job = jobs.get(jobId);
        if (job == null) {
            return status(jobId);
        }
        job.cancelled.set(true);
        job.status = "CANCELLED";
        job.currentStage = "CANCELLED";
        job.message = "已请求取消，后台会在当前批次结束后停止";
        job.finishedAt.compareAndSet(0, System.currentTimeMillis());
        return snapshot(job);
    }

    private void runJob(SeedJob job) {
        job.status = "RUNNING";
        job.currentStage = "SENDING";
        job.message = "正在通过 Nginx Lua 上报埋点";
        long baseSecond = Instant.now().minus(1, ChronoUnit.MINUTES).getEpochSecond() / 60 * 60;
        try {
            for (int i = 1; i <= job.users && !job.cancelled.get(); i++) {
                String userId = "ui_user_%07d".formatted(i);
                String deviceId = "ui_device_%07d".formatted(i);
                String sessionId = "ui_session_%d_%d".formatted(baseSecond, i);
                String productId = "sku_%03d".formatted((i % 200) + 1);
                int amount = 99 + (i % 20) * 10;
                int offset = i % 50;

                send(job, "%s_%d_page_view".formatted(job.eventPrefix, i), userId, deviceId, sessionId, "page_view", baseSecond + offset, "/home", productId, 0);
                if (i <= job.users * 70L / 100L) {
                    send(job, "%s_%d_product_view".formatted(job.eventPrefix, i), userId, deviceId, sessionId, "product_view", baseSecond + offset + 4, "/product/" + productId, productId, 0);
                }
                if (i <= job.users * 45L / 100L) {
                    send(job, "%s_%d_add_cart".formatted(job.eventPrefix, i), userId, deviceId, sessionId, "add_cart", baseSecond + offset + 8, "/cart", productId, amount);
                }
                if (i <= job.users * 25L / 100L) {
                    send(job, "%s_%d_checkout".formatted(job.eventPrefix, i), userId, deviceId, sessionId, "checkout", baseSecond + offset + 12, "/checkout", productId, amount);
                }
                if (i <= job.users * 15L / 100L) {
                    send(job, "%s_%d_pay_success".formatted(job.eventPrefix, i), userId, deviceId, sessionId, "pay_success", baseSecond + offset + 16, "/pay/success", productId, amount);
                }

                if (i % job.batchSize == 0) {
                    job.currentStage = "SENT_%d_USERS".formatted(i);
                    refreshDorisCounts(job);
                    if (job.pauseMs > 0) {
                        Thread.sleep(job.pauseMs);
                    }
                }
            }

            if (!job.cancelled.get()) {
                job.currentStage = "ADVANCING_WATERMARK";
                send(job, "%s_watermark".formatted(job.eventPrefix), "ui_watermark_user", "ui_watermark_device",
                        "ui_watermark_session", "page_view", Instant.now().getEpochSecond(), "/watermark", "sku_000", 0);
                job.status = "FINISHED";
                job.currentStage = "FINISHED";
                job.message = "发送完成，Flink/Doris 可能仍在追赶聚合窗口";
            }
        } catch (Exception e) {
            job.status = "FAILED";
            job.currentStage = "FAILED";
            job.message = e.getMessage();
        } finally {
            job.finishedAt.compareAndSet(0, System.currentTimeMillis());
            refreshDorisCounts(job);
        }
    }

    private void send(SeedJob job, String eventId, String userId, String deviceId, String sessionId, String eventName,
                      long eventSecond, String page, String productId, int amount) {
        Map<String, Object> body = Map.of(
                "eventId", eventId,
                "appId", "demo-app",
                "userId", userId,
                "deviceId", deviceId,
                "sessionId", sessionId,
                "eventName", eventName,
                "eventTime", eventSecond * 1000,
                "properties", Map.of(
                        "page", page,
                        "productId", productId,
                        "amount", amount,
                        "channel", "ui-seed-job"));
        try {
            restClient.post()
                    .uri(ingestEndpoint)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .body(String.class);
            job.sentEvents.incrementAndGet();
            job.updatedAt.set(System.currentTimeMillis());
        } catch (Exception e) {
            job.failedEvents.incrementAndGet();
            job.updatedAt.set(System.currentTimeMillis());
        }
    }

    private void refreshDorisCounts(SeedJob job) {
        try {
            long now = System.currentTimeMillis();
            long previousDwdDelta = job.dwdCount.get();
            Long dwd = jdbcTemplate.queryForObject("select count(*) from dwd_event_detail where app_id='demo-app' and event_date=curdate() and event_id like ?", Long.class, job.eventPrefix + "%");
            Long dws = jdbcTemplate.queryForObject("select count(*) from dws_event_1m where app_id='demo-app' and window_start >= curdate()", Long.class);
            job.dwdCount.set(dwd == null ? 0 : dwd);
            job.dwsCount.set(dws == null ? 0 : dws);
            if (job.dwdCount.get() > previousDwdDelta) {
                job.lastDwdGrowthAt.set(now);
            }
            job.lastDwdDelta.set(job.dwdCount.get());
            job.lastDorisRefreshAt.set(now);
        } catch (Exception e) {
            job.message = "Doris 统计查询暂不可用: " + e.getMessage();
        }
    }

    private void refreshDorisCountsIfDue(SeedJob job) {
        long now = System.currentTimeMillis();
        if (now - job.lastDorisRefreshAt.get() >= 3_000) {
            refreshDorisCounts(job);
        }
    }

    private SeedJobSnapshotDto snapshot(SeedJob job) {
        long now = System.currentTimeMillis();
        long elapsedMs = Math.max(now - job.startedAt, 1);
        double sendTps = job.sentEvents.get() * 1000.0 / elapsedMs;
        double progress = Math.min(1.0, job.sentEvents.get() * 1.0 / Math.max(job.expectedEvents, 1));
        long dwdDelta = job.dwdCount.get();
        long dwsDelta = Math.max(job.dwsCount.get() - job.baselineDwsCount, 0);
        long dorisLag = Math.max(job.sentEvents.get() - dwdDelta, 0);
        String message = enrichMessage(job, dwdDelta, dorisLag, now);
        return new SeedJobSnapshotDto(
                job.jobId,
                job.status,
                job.users,
                job.expectedEvents,
                job.sentEvents.get(),
                job.failedEvents.get(),
                job.dwdCount.get(),
                job.dwsCount.get(),
                dwdDelta,
                dwsDelta,
                dorisLag,
                sendTps,
                progress,
                job.currentStage,
                message,
                job.startedAt,
                job.updatedAt.get(),
                job.finishedAt.get());
    }

    private String enrichMessage(SeedJob job, long dwdDelta, long dorisLag, long now) {
        if (job.sentEvents.get() == 0 || dorisLag == 0 || !"RUNNING".equals(job.status)) {
            return job.message;
        }
        long idleMs = now - job.lastDwdGrowthAt.get();
        if (idleMs > 15_000 && dwdDelta == job.lastDwdDelta.get()) {
            return "入口仍在发送，但 Doris DWD 本次新增暂未变化，请检查 Flink 作业是否运行";
        }
        return job.message;
    }

    private long expectedEvents(int users) {
        return users
                + users * 70L / 100L
                + users * 45L / 100L
                + users * 25L / 100L
                + users * 15L / 100L
                + 1L;
    }

    private static class SeedJob {
        private final String jobId;
        private final String eventPrefix;
        private final int users;
        private final long expectedEvents;
        private final int batchSize;
        private final int pauseMs;
        private final long startedAt = System.currentTimeMillis();
        private final AtomicLong sentEvents = new AtomicLong();
        private final AtomicLong failedEvents = new AtomicLong();
        private final AtomicLong dwdCount = new AtomicLong();
        private final AtomicLong dwsCount = new AtomicLong();
        private final AtomicLong lastDwdDelta = new AtomicLong();
        private final AtomicLong lastDwdGrowthAt = new AtomicLong(startedAt);
        private final AtomicLong lastDorisRefreshAt = new AtomicLong();
        private final AtomicLong updatedAt = new AtomicLong(startedAt);
        private final AtomicLong finishedAt = new AtomicLong();
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private volatile long baselineDwdCount;
        private volatile long baselineDwsCount;
        private volatile String status = "CREATED";
        private volatile String currentStage = "CREATED";
        private volatile String message = "任务已创建";

        private SeedJob(String jobId, int users, long expectedEvents, int batchSize, int pauseMs) {
            this.jobId = jobId;
            this.eventPrefix = "ui_" + jobId.replace("-", "_");
            this.users = users;
            this.expectedEvents = expectedEvents;
            this.batchSize = batchSize;
            this.pauseMs = pauseMs;
        }
    }
}
