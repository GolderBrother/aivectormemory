<script lang="ts" setup>
defineProps<{ type?: 'warning' | 'info' | 'success' | 'muted' }>()
</script>

<template>
  <span class="badge" :class="`badge--${type || 'info'}`">
    <slot />
  </span>
</template>

<style scoped>
.badge {
  display: inline-flex; align-items: center; padding: 3px 10px;
  border-radius: var(--radius-pill); font-size: var(--font-size-xs);
  font-weight: 600; letter-spacing: 0.2px;
}
.badge--warning { background: var(--color-warning-bg); color: var(--color-warning); }
.badge--info { background: var(--color-info-bg); color: var(--color-info); }
.badge--success { background: var(--color-success-bg); color: var(--color-success); }
.badge--muted { background: rgba(148,163,184,0.1); color: var(--text-muted); }
</style>
