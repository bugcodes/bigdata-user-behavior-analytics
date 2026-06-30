<template>
  <main class="portfolio-shell" :data-theme="resolvedTheme">
    <nav id="top" class="portfolio-nav">
      <a class="brand" href="/">
        <span>zhaobinjie</span>
        <small>Java / Realtime Data / AI</small>
      </a>
      <div class="nav-actions">
        <a href="#ai">AI</a>
        <a href="#opensource">开源</a>
        <a href="#stack">技术栈</a>
        <a href="#contact">联系</a>
        <div class="theme-switcher" aria-label="背景主题">
          <button
            v-for="option in themeOptions"
            :key="option.value"
            :class="{ active: themeMode === option.value }"
            :title="`切换为${option.label}`"
            @click="setTheme(option.value)"
          >
            <component :is="option.icon" :size="15" />
          </button>
        </div>
        <a class="nav-demo" href="/bigdata">
          <MonitorUp :size="16" />
          Demo
        </a>
      </div>
    </nav>

    <section class="profile-section">
      <div class="hero-profile">
        <p>
          <strong>后端技术：</strong>熟练掌握 Java 与分布式原理，精通 Spring 全家桶（Spring Boot、Spring Cloud，含 Nacos、Gateway、OpenFeign）微服务体系；熟悉 MySQL、Redis、Elasticsearch、Kafka、Nginx、Netty，具备分库分表与性能调优经验。
        </p>
        <p>
          <strong>大数据技术：</strong>熟悉 CDH 与 Doris 两套大数据架构，具备海量行为数据的存储与实时 / 离线分析能力，行为数据流量日均 3 亿；熟悉基于 Hive、Spark、Flink 的 ETL 数据处理与实时计算链路。
        </p>
        <p>
          <strong>AI方向：</strong>基于已有产品落地「AI 一本通」AI Copilot 产品，熟悉其完整实现（Flask + OpenAI SDK + 手动编排实现多轮会话等能力）；熟悉 LangChain、Harness 等大模型应用框架；深度使用 OpenClaw 及各类 AI 编程 / Agent 工具，持续将 AI 能力融入研发与产品。
        </p>
        <p>
          具备团队管理经验，能独立负责产品后端从需求评审、排期到上线的端到端交付，以及 POC 支持与客户问题闭环。
        </p>
      </div>
    </section>

    <section class="portfolio-hero">
      <div class="hero-copy">
        <div class="hero-demo-copy">
          <p class="demo-label">用户行为分析实时数仓 Demo</p>
          <p>
            我正在构建一套可在线演示的实时数仓项目，覆盖 Nginx Lua 采集、Kafka 缓冲、Flink 清洗聚合、Doris 查询和 Spring Boot API。
          </p>
          <p>
            这不是静态假页面。Demo 可以从前端触发造数，请求进入 Nginx Lua 采集层，再写入 Kafka，由 Flink 做清洗、去重、Watermark、分钟窗口聚合，最终落到 Doris 的 DWD/DWS 表。
          </p>
          <ul>
            <li>后端 API 直接查询 Doris，不走 mock 数据。</li>
            <li>前端可以观察造数进度、DWD/DWS 变化和转化漏斗。</li>
            <li>部署在阿里云 ECS，用 Docker 管理 Kafka、Doris、Flink 和采集入口。</li>
          </ul>
        </div>
        <div class="hero-actions">
          <a class="primary-link" href="/bigdata">
            <PlayCircle :size="18" />
            查看实时数仓 Demo
          </a>
          <a class="secondary-link" href="#stack">
            <ServerCog :size="18" />
            查看技术栈
          </a>
        </div>
      </div>

      <aside class="status-console" aria-label="项目运行状态">
        <div class="console-head">
          <div>
            <span class="status-dot"></span>
            <strong>用户行为分析系统</strong>
          </div>
          <small>ONLINE</small>
        </div>
        <div class="console-metrics">
          <div>
            <span>DWD 明细</span>
            <strong>511 rows</strong>
          </div>
          <div>
            <span>DWS 聚合</span>
            <strong>510 pv</strong>
          </div>
          <div>
            <span>实时链路</span>
            <strong>7 stages</strong>
          </div>
        </div>
        <div class="console-flow">
          <div v-for="node in architecture" :key="node.name" class="arch-node console-flow-node">
            <component :is="node.icon" :size="20" />
            <div>
              <strong>{{ node.name }}</strong>
              <span>{{ node.desc }}</span>
            </div>
          </div>
        </div>
      </aside>
    </section>

    <section id="ai" class="ai-section">
      <div class="section-heading">
        <div>
          <p>AI 工程实践</p>
          <h2>从 RAG、Agent 到工程复刻的持续探索</h2>
        </div>
        <a class="section-back" href="#top" title="回到顶部">
          <ArrowUp :size="16" />
          顶部
        </a>
      </div>
      <div class="ai-grid">
        <a v-for="item in aiPractices" :key="item.title" class="ai-card" :href="item.url" target="_blank" rel="noreferrer">
          <component :is="item.icon" :size="22" />
          <span>{{ item.tag }}</span>
          <h3>{{ item.title }}</h3>
          <p>{{ item.desc }}</p>
          <strong>
            {{ item.action }}
            <ExternalLink :size="15" />
          </strong>
        </a>
      </div>
    </section>

    <section id="opensource" class="opensource-section">
      <div class="section-heading">
        <div>
          <p>开源与内容作品</p>
          <h2>把 AI 工具使用、源码阅读和知识整理沉淀成公开作品</h2>
        </div>
        <a class="section-back" href="#top" title="回到顶部">
          <ArrowUp :size="16" />
          顶部
        </a>
      </div>
      <div class="opensource-layout">
        <a class="featured-repo" href="https://github.com/bugcodes/apollo-use-cases" target="_blank" rel="noreferrer">
          <div>
            <GitFork :size="22" />
            <span>GitHub / Apollo</span>
          </div>
          <h3>apollo-use-cases</h3>
          <p>参与 Apollo 配置中心使用场景示例，覆盖动态路由、动态日志、动态数据源、限流规则等 Java/Spring 场景。</p>
          <strong>
            查看仓库
            <ExternalLink :size="15" />
          </strong>
        </a>

        <div class="works-grid">
          <a v-for="work in publicWorks" :key="work.title" class="work-card" :href="work.url" target="_blank" rel="noreferrer">
            <component :is="work.icon" :size="19" />
            <div>
              <h3>{{ work.title }}</h3>
              <p>{{ work.desc }}</p>
            </div>
            <ExternalLink :size="15" />
          </a>
        </div>
      </div>
    </section>

    <section id="stack" class="stack-section">
      <div class="section-heading">
        <div>
          <p>能力结构</p>
          <h2>围绕真实链路组织技术栈</h2>
        </div>
        <a class="section-back" href="#top" title="回到顶部">
          <ArrowUp :size="16" />
          顶部
        </a>
      </div>
      <div class="stack-grid">
        <article v-for="group in stackGroups" :key="group.title" class="stack-card">
          <component :is="group.icon" :size="22" />
          <h3>{{ group.title }}</h3>
          <p>{{ group.desc }}</p>
          <div>
            <span v-for="item in group.items" :key="item">{{ item }}</span>
          </div>
        </article>
      </div>
    </section>

    <section id="contact" class="contact-band">
      <div>
        <p>求职方向</p>
        <h2>Java后端 / 大数据开发 / AI应用开发</h2>
      </div>
      <div class="contact-actions">
        <a class="primary-link" href="/bigdata">
          <MonitorUp :size="18" />
          进入 Demo
        </a>
        <a class="secondary-link" href="mailto:">
          <Mail :size="18" />
          联系我
        </a>
      </div>
    </section>
    <a class="floating-top" href="#top" title="回到顶部">
      <ArrowUp :size="18" />
    </a>
  </main>
</template>

<script setup lang="ts">
import {
  Activity,
  ArrowUp,
  Bot,
  BookOpen,
  Braces,
  Code2,
  Database,
  ExternalLink,
  GitFork,
  Laptop,
  Mail,
  MonitorUp,
  Moon,
  PlayCircle,
  Puzzle,
  Router,
  ServerCog,
  Sun,
  WandSparkles,
  Waves
} from 'lucide-vue-next';
import { computed, onBeforeUnmount, onMounted, ref } from 'vue';
import { sendTracePayload } from '../api/analytics';

type ThemeMode = 'system' | 'light' | 'dark';

const themeOptions = [
  { value: 'system' as const, label: '系统色', icon: Laptop },
  { value: 'light' as const, label: '浅色', icon: Sun },
  { value: 'dark' as const, label: '深色', icon: Moon }
];

const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
const themeMode = ref<ThemeMode>((localStorage.getItem('portfolio-theme') as ThemeMode) || 'system');
const systemTheme = ref<'light' | 'dark'>(mediaQuery.matches ? 'dark' : 'light');
const resolvedTheme = computed(() => (themeMode.value === 'system' ? systemTheme.value : themeMode.value));

function setTheme(value: ThemeMode) {
  themeMode.value = value;
  localStorage.setItem('portfolio-theme', value);
}

function syncSystemTheme(event: MediaQueryListEvent) {
  systemTheme.value = event.matches ? 'dark' : 'light';
}

function getOrCreateVisitorId() {
  const key = 'portfolio-visitor-id';
  const existing = localStorage.getItem(key);
  if (existing) {
    return existing;
  }
  const id = `portfolio_${crypto.randomUUID ? crypto.randomUUID() : `${Date.now()}_${Math.random().toString(16).slice(2)}`}`;
  localStorage.setItem(key, id);
  return id;
}

async function trackHomeView() {
  const now = Date.now();
  const visitorId = getOrCreateVisitorId();
  const sessionKey = 'portfolio-session-id';
  const sessionId = sessionStorage.getItem(sessionKey) || `session_${now}_${Math.random().toString(16).slice(2)}`;
  sessionStorage.setItem(sessionKey, sessionId);

  try {
    await sendTracePayload({
      appId: 'demo-app',
      userId: visitorId,
      deviceId: visitorId,
      sessionId,
      eventName: 'page_view',
      eventTime: now,
      properties: {
        page: '/',
        title: document.title,
        channel: 'portfolio-home',
        referrer: document.referrer || '',
        path: window.location.pathname
      }
    });
  } catch (error) {
    console.warn('home view trace failed', error);
  }
}

onMounted(() => {
  mediaQuery.addEventListener('change', syncSystemTheme);
  trackHomeView();
});

onBeforeUnmount(() => {
  mediaQuery.removeEventListener('change', syncSystemTheme);
});

const architecture = [
  { name: 'Nginx + Lua', desc: '埋点接入与协议校验', icon: Router },
  { name: 'Kafka', desc: '行为事件缓冲与削峰', icon: Waves },
  { name: 'Flink', desc: '清洗、去重、窗口聚合', icon: Activity },
  { name: 'Doris', desc: 'DWD 明细与 DWS 聚合查询', icon: Database },
  { name: 'Spring Boot + Vue', desc: 'API 查询与实时看板', icon: MonitorUp }
];

const aiPractices = [
  {
    tag: 'RAG / Knowledge Base',
    title: 'RAG 问答与知识库项目',
    desc: '真实 RAG 工作台项目，覆盖知识库管理、文档切分、向量召回、上下文拼接和问答生成链路。',
    icon: Bot,
    url: 'https://github.com/bugcodes/rag-workspace',
    action: '查看 rag-workspace'
  },
  {
    tag: 'Agent Engineering',
    title: 'OpenClaw / Hermes 工程复刻',
    desc: '基于 Java 复刻 Agent Harness 工程骨架，关注工具调用、任务编排、执行循环和本地自动化体验。',
    icon: Puzzle,
    url: 'https://github.com/bugcodes/agent-harness-java',
    action: '查看 agent-harness-java'
  },
  {
    tag: 'AI Workflow',
    title: 'AI 辅助内容与工具生产',
    desc: '用 AI 工具持续产出指南、资源索引、工具集合和视觉内容，把使用经验产品化、文档化。',
    icon: WandSparkles,
    url: '#opensource',
    action: '查看公开作品'
  }
];

const publicWorks = [
  {
    title: 'book-of-elon-zh',
    desc: 'The Book of Elon 中文版整理。',
    url: 'https://bugcodes.github.io/book-of-elon-zh/',
    icon: BookOpen
  },
  {
    title: 'devzen',
    desc: 'AI 时代的极客生存战力修行场。',
    url: 'https://bugcodes.github.io/devzen/',
    icon: Code2
  },
  {
    title: 'AIGuide',
    desc: 'AI Agent 学习指南。',
    url: 'https://bugcodes.github.io/AIGuide/',
    icon: Bot
  },
  {
    title: 'ai-tools-hub',
    desc: '常用 AI 工具导航与整理。',
    url: 'https://bugcodes.github.io/ai-tools-hub/',
    icon: WandSparkles
  },
  {
    title: 'ai-wallpaper',
    desc: 'AI 壁纸生成与展示项目。',
    url: 'https://bugcodes.github.io/ai-wallpaper/',
    icon: MonitorUp
  },
  {
    title: 'hermes-agent',
    desc: 'Hermes Agent 完全指南。',
    url: 'https://bugcodes.github.io/hermes-agent/',
    icon: Puzzle
  }
];

const stackGroups = [
  {
    title: '后端服务',
    desc: '围绕实时看板提供查询、造数任务和链路进度接口。',
    icon: Braces,
    items: ['Java 17', 'Spring Boot', 'REST API', 'JDBC']
  },
  {
    title: '实时计算',
    desc: '处理事件时间、迟到数据、状态去重和分钟级聚合。',
    icon: Waves,
    items: ['Kafka', 'Flink', 'Watermark', 'Stream Load']
  },
  {
    title: '分析存储',
    desc: '用 Doris 承接明细表、聚合表和前端分析查询。',
    icon: Database,
    items: ['Doris', 'DWD', 'DWS', 'Unique Key']
  },
  {
    title: '部署运维',
    desc: '在云服务器上组织前端、后端和大数据组件的演示环境。',
    icon: ServerCog,
    items: ['Docker', 'Nginx', 'Linux', 'Alibaba Cloud']
  }
];
</script>
