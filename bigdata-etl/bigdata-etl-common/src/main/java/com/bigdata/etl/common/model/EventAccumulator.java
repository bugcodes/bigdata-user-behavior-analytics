package com.bigdata.etl.common.model;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * 窗口聚合累加器，负责在分钟窗口内累计 PV、UV、用户数与处理延迟。
 *
 * @author zhaobinjie
 * @date 2026-06-25
 */
public class EventAccumulator implements Serializable {

    private long pv;
    private long latencyTotalMs;
    private Set<String> devices = new HashSet<>();
    private Set<String> users = new HashSet<>();

    public void add(CleanEvent event) {
        pv++;
        latencyTotalMs += event.processLatencyMs();
        devices.add(event.deviceId());
        users.add(event.userId());
    }

    public EventAccumulator merge(EventAccumulator other) {
        pv += other.pv;
        latencyTotalMs += other.latencyTotalMs;
        devices.addAll(other.devices);
        users.addAll(other.users);
        return this;
    }

    public long pv() {
        return pv;
    }

    public long uv() {
        return devices.size();
    }

    public long users() {
        return users.size();
    }

    public double avgLatencyMs() {
        return pv == 0 ? 0 : (double) latencyTotalMs / pv;
    }
}
