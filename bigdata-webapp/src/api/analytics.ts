import axios from 'axios';

export interface TrendPoint {
  minute: string;
  pv: number;
  uv: number;
  tps: number;
}

export interface EventMetric {
  eventName: string;
  pv: number;
  uv: number;
  users: number;
  avgLatencyMs: number;
}

export interface FunnelStep {
  step: string;
  users: number;
  conversionRate: number;
}

export interface RealtimeSummary {
  pv: number;
  uv: number;
  fastPv: number;
  fastUv: number;
  dau: number;
  ingestTps: number;
  p99LatencyMs: number;
  errorRate: number;
  trend: TrendPoint[];
  topEvents: EventMetric[];
  funnel: FunnelStep[];
}

export interface SeedJobRequest {
  users: number;
  batchSize: number;
  pauseMs: number;
}

export interface SeedJobSnapshot {
  jobId: string;
  status: string;
  users: number;
  expectedEvents: number;
  sentEvents: number;
  failedEvents: number;
  dwdCount: number;
  dwsCount: number;
  dwdDelta: number;
  dwsDelta: number;
  dorisLag: number;
  sendTps: number;
  progress: number;
  currentStage: string;
  message: string;
  startedAt: number;
  updatedAt: number;
  finishedAt: number;
}

interface ApiResponse<T> {
  code: string;
  msg: string;
  data: T;
}

export async function fetchRealtimeSummary() {
  const response = await axios.get<ApiResponse<RealtimeSummary>>('/api/analytics/realtime');
  return response.data.data;
}

export async function sendTraceEvent(eventName: string) {
  const now = Date.now();
  return sendTracePayload({
    appId: 'demo-app',
    userId: `u_${Math.floor(Math.random() * 100000)}`,
    deviceId: `d_${Math.floor(Math.random() * 100000)}`,
    sessionId: `s_${now}`,
    eventName,
    eventTime: now,
    properties: {
      page: '/demo',
      channel: 'codex-local',
      price: Math.round(Math.random() * 1000)
    }
  });
}

export interface TracePayload {
  appId: string;
  userId: string;
  deviceId: string;
  sessionId: string;
  eventName: string;
  eventTime: number;
  properties: Record<string, unknown>;
}

export async function sendTracePayload(payload: TracePayload) {
  const response = await axios.post<ApiResponse<string>>('/api/analytics/trace', payload);
  return response.data.data;
}

export async function startSeedJob(request: SeedJobRequest) {
  const response = await axios.post<ApiResponse<SeedJobSnapshot>>('/api/analytics/seed/start', request);
  return response.data.data;
}

export async function fetchSeedJobStatus(jobId: string) {
  const response = await axios.get<ApiResponse<SeedJobSnapshot>>(`/api/analytics/seed/status/${jobId}`);
  return response.data.data;
}

export async function cancelSeedJob(jobId: string) {
  const response = await axios.post<ApiResponse<SeedJobSnapshot>>(`/api/analytics/seed/cancel/${jobId}`);
  return response.data.data;
}
