<template>
  <DashboardView v-if="isDemoRoute" />
  <PortfolioHome v-else />
</template>

<script setup lang="ts">
import { computed } from 'vue';
import DashboardView from './views/DashboardView.vue';
import PortfolioHome from './views/PortfolioHome.vue';

const isDemoRoute = computed(() => window.location.pathname.startsWith('/bigdata'));
</script>
