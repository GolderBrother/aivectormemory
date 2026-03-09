<script lang="ts" setup>
import { useI18n } from 'vue-i18n'
import Badge from '../common/Badge.vue'

const { t } = useI18n()

const props = defineProps<{
  task: any
  readonly?: boolean
}>()

const emit = defineEmits<{
  (e: 'toggle', task: any): void
  (e: 'edit', task: any): void
  (e: 'delete', task: any): void
}>()

const statusIcon: Record<string, string> = {
  pending: '\u25CB',
  in_progress: '\u25D4',
  completed: '\u2714',
  done: '\u2714',
  skipped: '\u2718',
  archived: '\u25CF',
}

const statusBadge: Record<string, 'warning' | 'info' | 'success' | 'muted'> = {
  pending: 'warning',
  in_progress: 'info',
  completed: 'success',
  done: 'success',
  skipped: 'muted',
  archived: 'muted',
}
</script>

<template>
  <div :class="['task-item', `status--${task.status}`]">
    <span
      class="task-checkbox"
      @click.stop="!readonly && emit('toggle', task)"
    >{{ statusIcon[task.status] || '\u25CB' }}</span>
    <span class="task-title">{{ task.title }}</span>
    <Badge v-if="task.status && task.status !== 'pending'" :type="statusBadge[task.status] || 'muted'">
      {{ t(`status.${task.status}`) }}
    </Badge>
    <div v-if="!readonly" class="task-actions-group" @click.stop>
      <button class="btn btn--ghost btn--sm" @click="emit('edit', task)">{{ t('edit') }}</button>
      <button class="btn btn--ghost-danger btn--sm" @click="emit('delete', task)">{{ t('delete') }}</button>
    </div>
  </div>
</template>

<style scoped>
.task-item {
  display: flex; align-items: center; gap: 10px;
  padding: 8px 16px; transition: background 0.15s; cursor: default;
}
.task-item:hover { background: var(--accent-bg-subtle); }
.task-checkbox {
  cursor: pointer; font-size: 16px; color: var(--text-muted);
  user-select: none; flex-shrink: 0;
}
.task-item.status--done .task-checkbox,
.task-item.status--completed .task-checkbox { color: var(--color-success-dot); }
.task-item.status--skipped .task-checkbox { color: var(--color-error); }
.task-title { font-size: 13px; color: var(--text-heading); flex: 1; }
.task-actions-group { display: flex; gap: 4px; opacity: 0; transition: opacity 0.15s; }
.task-item:hover .task-actions-group { opacity: 1; }
</style>
