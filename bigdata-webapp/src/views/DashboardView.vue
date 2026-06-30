<template>
  <main class="dashboard">
    <header class="topbar">
      <div>
        <a class="back-home-link" href="/" title="返回个人首页">
          <ArrowLeft :size="16" />
          返回首页
        </a>
        <h1>用户行为分析系统</h1>
        <p>日 10 亿埋点接入链路 · Nginx Lua / Kafka / Flink / Doris</p>
      </div>
      <div class="actions">
        <button @click="loadData" :disabled="loading" title="刷新实时指标">
          <RefreshCw :size="17" />
          刷新
        </button>
        <button class="primary" @click="trace('pay_success')" title="发送一条支付成功测试埋点">
          <Send :size="17" />
          模拟埋点
        </button>
      </div>
    </header>

    <section class="metrics-grid">
      <MetricCard title="今日 PV" :value="formatNumber(realtimePv)" hint="首页访问秒级计数" :icon="Activity" />
      <MetricCard title="今日 UV" :value="formatNumber(realtimeUv)" hint="访客去重秒级计数" :icon="Users" />
      <MetricCard title="DAU" :value="formatNumber(summary.dau)" hint="用户去重口径" :icon="UserRoundCheck" />
      <MetricCard title="接入 TPS" :value="summary.ingestTps.toFixed(1)" hint="最近 1 分钟" :icon="Gauge" />
      <MetricCard title="P99 延迟" :value="`${summary.p99LatencyMs.toFixed(1)}ms`" hint="端到端处理" :icon="Timer" />
      <MetricCard title="错误率" :value="`${(summary.errorRate * 100).toFixed(3)}%`" hint="实时质量校验" :icon="ShieldCheck" />
    </section>

    <section class="panel seed-panel">
      <div class="panel-title">
        <h2>批量造数链路监控</h2>
        <span>{{ seedSnapshot ? seedSnapshot.status : 'READY' }}</span>
      </div>
      <div class="seed-layout">
        <div class="seed-controls">
          <p class="seed-hint">默认演示规格：1 万用户，预计约 2.55 万条事件；如需百万级压测建议在线下或扩容后执行。</p>
          <label>
            <span>用户数</span>
            <input v-model.number="seedForm.users" type="number" min="1" max="100000" />
          </label>
          <label>
            <span>批大小</span>
            <input v-model.number="seedForm.batchSize" type="number" min="1" max="20000" />
          </label>
          <label>
            <span>批间隔 ms</span>
            <input v-model.number="seedForm.pauseMs" type="number" min="0" max="10000" />
          </label>
          <div class="seed-actions">
            <button class="primary" @click="startSeed" :disabled="seedRunning" title="启动后台批量造数任务">
              <Play :size="17" />
              启动造数
            </button>
            <button @click="cancelSeed" :disabled="!seedRunning || !seedSnapshot" title="取消当前造数任务">
              <Square :size="17" />
              取消
            </button>
          </div>
        </div>

        <div class="seed-progress">
          <div class="progress-head">
            <strong>{{ seedSnapshot ? formatNumber(seedSnapshot.sentEvents) : '0' }}</strong>
            <span>/ {{ seedSnapshot ? formatNumber(seedSnapshot.expectedEvents) : expectedSeedEventsText }} events</span>
          </div>
          <div class="bar progress-bar">
            <i :style="{ width: `${Math.max((seedSnapshot?.progress || 0) * 100, seedSnapshot ? 1 : 0)}%` }"></i>
          </div>
          <div class="seed-stats">
            <span>发送 TPS {{ seedSnapshot ? seedSnapshot.sendTps.toFixed(1) : '0.0' }}</span>
            <span>失败 {{ seedSnapshot ? formatNumber(seedSnapshot.failedEvents) : '0' }}</span>
            <span>本次 DWD +{{ seedSnapshot ? formatNumber(seedSnapshot.dwdDelta) : '0' }}</span>
            <span>本次 DWS +{{ seedSnapshot ? formatNumber(seedSnapshot.dwsDelta) : '0' }}</span>
            <span>待入 DWD {{ seedSnapshot ? formatNumber(seedSnapshot.dorisLag) : '0' }}</span>
          </div>
          <p class="seed-message">{{ seedSnapshot?.message || '默认按演示规格造数，启动后可观察埋点进入 Nginx、Kafka、Flink，再写入 Doris。' }}</p>
        </div>
      </div>

      <div class="pipeline">
        <div v-for="node in pipelineNodes" :key="node.key" class="pipeline-node" :class="node.state">
          <component :is="node.icon" :size="18" />
          <span>{{ node.label }}</span>
          <strong>{{ node.value }}</strong>
        </div>
      </div>
    </section>

    <section class="workbench">
      <div class="panel large">
        <div class="panel-title">
          <h2>实时趋势</h2>
          <span>按事件时间窗口 · 最近 30 分钟</span>
        </div>
        <div ref="trendRef" class="chart"></div>
      </div>

      <div class="panel">
        <div class="panel-title">
          <h2>转化漏斗</h2>
          <span>今日</span>
        </div>
        <div class="funnel-list">
          <div v-for="step in summary.funnel" :key="step.step" class="funnel-row">
            <div class="row-head">
              <span>{{ step.step }}</span>
              <strong>{{ formatNumber(step.users) }}</strong>
            </div>
            <div class="bar">
              <i :style="{ width: `${Math.max(step.conversionRate * 100, 2)}%` }"></i>
            </div>
            <small>{{ (step.conversionRate * 100).toFixed(1) }}%</small>
          </div>
        </div>
      </div>
    </section>

    <section class="panel table-panel">
      <div class="panel-title">
        <h2>Top 事件</h2>
        <span>按 PV 排序</span>
      </div>
      <table>
        <thead>
          <tr>
            <th>事件</th>
            <th>PV</th>
            <th>UV</th>
            <th>用户数</th>
            <th>平均延迟</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="event in summary.topEvents" :key="event.eventName">
            <td>{{ event.eventName }}</td>
            <td>{{ formatNumber(event.pv) }}</td>
            <td>{{ formatNumber(event.uv) }}</td>
            <td>{{ formatNumber(event.users) }}</td>
            <td>{{ event.avgLatencyMs.toFixed(1) }}ms</td>
          </tr>
        </tbody>
      </table>
    </section>
  </main>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, reactive, ref } from 'vue';
import * as echarts from 'echarts/core';
import { LineChart } from 'echarts/charts';
import { GridComponent, TooltipComponent } from 'echarts/components';
import { CanvasRenderer } from 'echarts/renderers';
import { Activity, ArrowLeft, Database, Gauge, Play, RefreshCw, Router, Send, ShieldCheck, Square, Timer, UserRoundCheck, Users, Waves } from 'lucide-vue-next';
import MetricCard from '../components/MetricCard.vue';
import {
  cancelSeedJob,
  fetchRealtimeSummary,
  fetchSeedJobStatus,
  sendTraceEvent,
  startSeedJob,
  type RealtimeSummary,
  type SeedJobSnapshot
} from '../api/analytics';

echarts.use([LineChart, GridComponent, TooltipComponent, CanvasRenderer]);

const loading = ref(false);
const seedRunning = ref(false);
const seedSnapshot = ref<SeedJobSnapshot | null>(null);
const trendRef = ref<HTMLDivElement | null>(null);
let trendChart: echarts.ECharts | null = null;
let seedTimer: number | undefined;
let refreshTimer: number | undefined;

const summary = reactive<RealtimeSummary>({
  pv: 0,
  uv: 0,
  fastPv: 0,
  fastUv: 0,
  dau: 0,
  ingestTps: 0,
  p99LatencyMs: 0,
  errorRate: 0,
  trend: [],
  topEvents: [],
  funnel: []
});

const seedForm = reactive({
  users: 10000,
  batchSize: 1000,
  pauseMs: 0
});

const expectedSeedEventsText = computed(() => formatNumber(expectedEvents(seedForm.users)));
const realtimePv = computed(() => summary.fastPv || summary.pv);
const realtimeUv = computed(() => summary.fastUv || summary.uv);
const pipelineNodes = computed(() => {
  const snapshot = seedSnapshot.value;
  const sent = snapshot?.sentEvents || 0;
  const dwd = snapshot?.dwdDelta || 0;
  const dws = snapshot?.dwsDelta || 0;
  const lag = snapshot?.dorisLag || 0;
  const frontendPv = realtimePv.value || 0;
  return [
    { key: 'generator', label: '生成器', value: formatNumber(sent), icon: Play, state: sent > 0 ? 'active' : 'idle' },
    { key: 'ingress', label: 'Nginx/Kafka', value: snapshot ? snapshot.currentStage : '待启动', icon: Router, state: sent > 0 ? 'active' : 'idle' },
    { key: 'flink', label: 'Flink ETL', value: lag > 0 ? `追赶 ${formatNumber(lag)}` : dwd > 0 ? '已追上' : '等待数据', icon: Waves, state: dwd > 0 ? 'active' : sent > 0 ? 'pending' : 'idle' },
    { key: 'doris', label: 'Doris DWD/DWS', value: `+${formatNumber(dwd)} / +${formatNumber(dws)}`, icon: Database, state: dws > 0 ? 'active' : dwd > 0 ? 'pending' : 'idle' },
    { key: 'frontend', label: '前端指标', value: formatNumber(frontendPv), icon: Activity, state: frontendPv > 0 ? 'active' : 'idle' }
  ];
});

function formatNumber(value: number) {
  return new Intl.NumberFormat('zh-CN').format(value);
}

async function loadData() {
  loading.value = true;
  try {
    Object.assign(summary, await fetchRealtimeSummary());
    await nextTick();
    renderTrend();
  } finally {
    loading.value = false;
  }
}

async function trace(eventName: string) {
  await sendTraceEvent(eventName);
  await loadData();
}

function expectedEvents(users: number) {
  return users + Math.floor(users * 0.7) + Math.floor(users * 0.45) + Math.floor(users * 0.25) + Math.floor(users * 0.15) + 1;
}

async function startSeed() {
  seedSnapshot.value = await startSeedJob({ ...seedForm });
  seedRunning.value = true;
  startSeedPolling();
}

async function cancelSeed() {
  if (!seedSnapshot.value) {
    return;
  }
  seedSnapshot.value = await cancelSeedJob(seedSnapshot.value.jobId);
  seedRunning.value = false;
  stopSeedPolling();
}

function startSeedPolling() {
  stopSeedPolling();
  seedTimer = window.setInterval(async () => {
    if (!seedSnapshot.value) {
      return;
    }
    seedSnapshot.value = await fetchSeedJobStatus(seedSnapshot.value.jobId);
    await loadData();
    if (['FINISHED', 'FAILED', 'CANCELLED', 'NOT_FOUND'].includes(seedSnapshot.value.status)) {
      seedRunning.value = false;
      stopSeedPolling();
    }
  }, 2000);
}

function stopSeedPolling() {
  if (seedTimer) {
    window.clearInterval(seedTimer);
    seedTimer = undefined;
  }
}

function renderTrend() {
  if (!trendRef.value) {
    return;
  }
  trendChart = trendChart || echarts.init(trendRef.value);
  trendChart.setOption({
    color: ['#2563eb', '#16a34a', '#f59e0b'],
    tooltip: { trigger: 'axis' },
    grid: { left: 46, right: 24, top: 30, bottom: 34 },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      data: summary.trend.map((item) => item.minute)
    },
    yAxis: [
      { type: 'value', name: 'PV/UV', splitLine: { lineStyle: { color: '#e5e7eb' } } },
      { type: 'value', name: 'TPS' }
    ],
    series: [
      { name: 'PV', type: 'line', smooth: true, areaStyle: { opacity: 0.08 }, data: summary.trend.map((item) => item.pv) },
      { name: 'UV', type: 'line', smooth: true, data: summary.trend.map((item) => item.uv) },
      { name: 'TPS', type: 'line', smooth: true, yAxisIndex: 1, data: summary.trend.map((item) => item.tps) }
    ]
  });
}

onMounted(() => {
  loadData();
  refreshTimer = window.setInterval(loadData, 10000);
  window.addEventListener('resize', () => trendChart?.resize());
});

onBeforeUnmount(() => {
  stopSeedPolling();
  if (refreshTimer) {
    window.clearInterval(refreshTimer);
    refreshTimer = undefined;
  }
});
</script>
