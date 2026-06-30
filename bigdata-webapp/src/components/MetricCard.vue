<template>
  <section class="metric-card">
    <div class="metric-icon">
      <component :is="icon" :size="18" />
    </div>
    <div>
      <p>{{ title }}</p>
      <strong>{{ value }}</strong>
      <span>{{ hint }}</span>
    </div>
  </section>
</template>

<script setup lang="ts">
import type { Component } from 'vue';

defineProps<{
  title: string;
  value: string;
  hint: string;
  icon: Component;
}>();
</script>
