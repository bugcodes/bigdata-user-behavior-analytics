package com.bigdata.etl.pipeline.sink;

import com.bigdata.etl.common.serde.JsonSerde;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Doris Stream Load 写入器，负责将 Flink 微批结果以 JSON Lines 方式写入 Doris。
 *
 * @author zhaobinjie
 * @date 2026-06-25
 */
public class DorisStreamLoadSink<T> extends RichSinkFunction<T> {

    private final String loadUrl;
    private final String username;
    private final String password;
    private final int batchSize;
    private final long flushIntervalMs;
    private transient HttpClient httpClient;
    private transient List<String> buffer;
    private transient long lastFlushTime;
    private transient ScheduledExecutorService flushExecutor;

    public DorisStreamLoadSink(String loadUrl, String username, String password, int batchSize, long flushIntervalMs) {
        this.loadUrl = loadUrl;
        this.username = username;
        this.password = password;
        this.batchSize = batchSize;
        this.flushIntervalMs = flushIntervalMs;
    }

    @Override
    public void open(Configuration parameters) {
        httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .followRedirects(HttpClient.Redirect.ALWAYS)
                .build();
        buffer = new ArrayList<>(batchSize);
        lastFlushTime = System.currentTimeMillis();
        flushExecutor = Executors.newSingleThreadScheduledExecutor();
        flushExecutor.scheduleWithFixedDelay(this::flushQuietly, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized void invoke(T value, Context context) throws Exception {
        buffer.add(JsonSerde.toJson(value));
        long now = System.currentTimeMillis();
        if (buffer.size() >= batchSize || now - lastFlushTime >= flushIntervalMs) {
            flush();
        }
    }

    @Override
    public void close() throws Exception {
        if (flushExecutor != null) {
            flushExecutor.shutdownNow();
        }
        flush();
    }

    private void flushQuietly() {
        try {
            flush();
        } catch (Exception e) {
            throw new IllegalStateException("Doris stream load periodic flush failed", e);
        }
    }

    private synchronized void flush() throws Exception {
        if (buffer == null || buffer.isEmpty()) {
            return;
        }
        String body = String.join("\n", buffer);
        HttpRequest request = HttpRequest.newBuilder(URI.create(loadUrl))
                .timeout(Duration.ofSeconds(10))
                .expectContinue(true)
                .header("Authorization", "Basic " + Base64.getEncoder()
                        .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8)))
                .header("label", "flink_" + UUID.randomUUID())
                .header("format", "json")
                .header("read_json_by_line", "true")
                .PUT(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 300) {
            throw new IllegalStateException("Doris stream load failed: " + response.body());
        }
        buffer.clear();
        lastFlushTime = System.currentTimeMillis();
    }
}
