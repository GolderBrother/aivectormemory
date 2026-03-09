<script lang="ts" setup>
import { computed } from 'vue'

const props = defineProps<{ total: number; page: number; pageSize: number }>()
const emit = defineEmits<{ 'page-change': [page: number] }>()

const totalPages = computed(() => Math.max(1, Math.ceil(props.total / props.pageSize)))

const pages = computed(() => {
  const t = totalPages.value
  const c = props.page
  const result: (number | '...')[] = []
  if (t <= 7) {
    for (let i = 1; i <= t; i++) result.push(i)
  } else {
    result.push(1)
    if (c > 3) result.push('...')
    for (let i = Math.max(2, c - 1); i <= Math.min(t - 1, c + 1); i++) result.push(i)
    if (c < t - 2) result.push('...')
    result.push(t)
  }
  return result
})

function go(p: number) {
  if (p >= 1 && p <= totalPages.value && p !== props.page) emit('page-change', p)
}
</script>

<template>
  <div v-if="total > pageSize" class="pager">
    <span class="pager__info">{{ total }} {{ $t('items') }}</span>
    <button
      v-for="(p, i) in pages"
      :key="i"
      class="pager__btn"
      :class="{ 'pager__btn--active': p === page }"
      :disabled="p === '...'"
      @click="typeof p === 'number' && go(p)"
    >
      <template v-if="p === '...'">
        <span class="pager__ellipsis">...</span>
      </template>
      <template v-else>{{ p }}</template>
    </button>
  </div>
</template>

<style scoped>
.pager {
  display: flex; align-items: center; flex-wrap: wrap; gap: 6px;
  margin-top: 20px; padding-top: 16px; border-top: 1px solid var(--border);
}
.pager__info {
  font-family: var(--font-mono); font-size: var(--font-size-sm);
  color: var(--text-muted); margin-right: 8px;
}
.pager__btn {
  padding: 6px 12px; border: 1px solid var(--border); border-radius: var(--radius-sm);
  background: var(--bg-surface); cursor: pointer; font-size: var(--font-size-base);
  color: var(--text-secondary); font-family: inherit; transition: all var(--transition-fast);
}
.pager__btn:hover:not(:disabled) { border-color: var(--accent); color: var(--accent-light); }
.pager__btn--active { background: var(--accent-hover); color: #fff; border-color: var(--accent-hover); }
.pager__btn:disabled { cursor: default; border-color: transparent; background: transparent; }
.pager__ellipsis { color: var(--text-muted); font-size: var(--font-size-base); }
</style>
