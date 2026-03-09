<script lang="ts" setup>
import { useI18n } from 'vue-i18n'

const { t } = useI18n()

defineProps<{ count: number }>()

const emit = defineEmits<{
  (e: 'merge'): void
  (e: 'delete'): void
  (e: 'cancel'): void
}>()
</script>

<template>
  <div v-if="count > 0" class="batch-bar">
    <span class="batch-bar__info">{{ t('selected') }} <strong>{{ count }}</strong> {{ t('tagsUnit') }}</span>
    <button class="btn btn--outline btn--sm" @click="emit('merge')">{{ t('batchMerge') }}</button>
    <button class="btn btn--danger btn--sm" @click="emit('delete')">{{ t('batchDelete') }}</button>
    <button class="btn btn--ghost btn--sm" @click="emit('cancel')">{{ t('cancelSelect') }}</button>
  </div>
</template>

<style scoped>
.batch-bar {
  display: flex; align-items: center; gap: 12px; padding: 12px 16px;
  background: var(--bg-surface); border-radius: var(--radius-md);
  border: 1px solid var(--border); margin-bottom: 16px;
}
.batch-bar__info { font-size: 13px; color: var(--text-secondary); }
.batch-bar__info strong { color: var(--text-primary); }
</style>
