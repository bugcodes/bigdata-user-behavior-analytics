package com.bigdata.backend.service;

import com.bigdata.backend.dto.RealtimeVisitCounterDto;
import com.bigdata.backend.dto.TraceEventRequest;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 首页访问实时计数服务，负责在数仓链路之外提供秒级 PV/UV 展示口径。
 *
 * @author zhaobinjie
 * @date 2026-06-30
 */
@Service
public class RealtimeVisitCounterService {

    private static final String HOME_PAGE_VIEW = "page_view";
    private static final String HOME_CHANNEL = "portfolio-home";
    private static final ZoneId ZONE_ID = ZoneId.of("Asia/Shanghai");

    private final ObjectMapper objectMapper;
    private final Path storagePath;
    private final Map<LocalDate, CounterState> states = new HashMap<>();

    public RealtimeVisitCounterService(
            ObjectMapper objectMapper,
            @Value("${analytics.realtime-counter.path:/data/app/bigdata-backend/runtime/realtime-visits.json}") String storagePath) {
        this.objectMapper = objectMapper;
        this.storagePath = Path.of(storagePath);
        load();
    }

    public synchronized RealtimeVisitCounterDto snapshot() {
        CounterState state = states.getOrDefault(LocalDate.now(ZONE_ID), new CounterState());
        return new RealtimeVisitCounterDto(state.pv(), state.visitors().size());
    }

    public synchronized RealtimeVisitCounterDto recordIfPortfolioHome(TraceEventRequest request) {
        if (!isPortfolioHomeView(request)) {
            return snapshot();
        }
        LocalDate today = LocalDate.now(ZONE_ID);
        CounterState state = states.computeIfAbsent(today, ignored -> new CounterState());
        state.increment(request.userId());
        persist();
        return new RealtimeVisitCounterDto(state.pv(), state.visitors().size());
    }

    private boolean isPortfolioHomeView(TraceEventRequest request) {
        Object channel = request.properties() == null ? null : request.properties().get("channel");
        return HOME_PAGE_VIEW.equals(request.eventName()) && HOME_CHANNEL.equals(channel);
    }

    private void load() {
        if (!Files.exists(storagePath)) {
            return;
        }
        try {
            Map<String, PersistedCounter> persisted = objectMapper.readValue(
                    Files.readString(storagePath, StandardCharsets.UTF_8),
                    new TypeReference<>() {
                    });
            persisted.forEach((date, counter) -> states.put(LocalDate.parse(date), counter.toState()));
        } catch (Exception ignored) {
            states.clear();
        }
    }

    private void persist() {
        Map<String, PersistedCounter> persisted = new HashMap<>();
        states.forEach((date, state) -> persisted.put(date.toString(), PersistedCounter.from(state)));
        try {
            Files.createDirectories(storagePath.getParent());
            objectMapper.writeValue(storagePath.toFile(), persisted);
        } catch (IOException e) {
            throw new IllegalStateException("persist realtime visit counter failed", e);
        }
    }

    private static final class CounterState {

        private long pv;
        private final Set<String> visitors;

        private CounterState() {
            this(0, new HashSet<>());
        }

        private CounterState(long pv, Set<String> visitors) {
            this.pv = pv;
            this.visitors = visitors;
        }

        private long pv() {
            return pv;
        }

        private Set<String> visitors() {
            return visitors;
        }

        private void increment(String visitorId) {
            pv++;
            visitors.add(visitorId);
        }
    }

    private record PersistedCounter(long pv, Set<String> visitors) {

        private static PersistedCounter from(CounterState state) {
            return new PersistedCounter(state.pv(), state.visitors());
        }

        private CounterState toState() {
            return new CounterState(pv, new HashSet<>(visitors));
        }
    }
}
