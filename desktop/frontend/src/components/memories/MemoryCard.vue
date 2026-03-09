<script lang="ts" setup>
import { ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { formatTime } from '../../utils/formatTime'

const { t } = useI18n()

const props = defineProps<{
  memory: {
    id: string
    content: string
    tags: string[]
    created_at: string
    scope?: string
  }
  selectable?: boolean
  selected?: boolean
}>()

const emit = defineEmits<{
  (e: 'edit', id: string): void
  (e: 'delete', id: string): void
  (e: 'toggle-select', id: string): void
}>()

const expanded = ref(false)

function onCardClick() {
  emit('edit', props.memory.id)
}
</script>

<template>
  <div class="memory-card" @click="onCardClick">
    <div class="memory-card__header">
      <div class="memory-card__header-left">
        <input
          v-if="selectable"
          type="checkbox"
          class="memory-card__check"
          :checked="selected"
          @click.stop
          @change.stop="emit('toggle-select', memory.id)"
        />
        <div class="memory-card__id">{{ memory.id }}</div>
        <div class="memory-card__tags">
          <span v-for="tag in memory.tags" :key="tag" class="tag">{{ tag }}</span>
        </div>
        <div class="memory-card__time">{{ formatTime(memory.created_at) }}</div>
      </div>
      <div class="memory-card__actions" @click.stop>
        <button class="btn btn--ghost btn--sm" @click="emit('edit', memory.id)">{{ t('edit') }}</button>
        <button class="btn btn--ghost-danger btn--sm" @click="emit('delete', memory.id)">{{ t('delete') }}</button>
      </div>
    </div>
    <div
      :class="['memory-card__content', expanded && 'expanded']"
      @click.stop="expanded = !expanded"
    >{{ memory.content }}</div>
  </div>
</template>

<style scoped>
.memory-card {
  background: var(--bg-surface); border-radius: var(--radius-md); padding: 18px 20px;
  border: 1px solid var(--border); transition: all 0.15s ease; cursor: pointer;
}
.memory-card:hover { border-color: var(--border-hover); box-shadow: var(--shadow-card); }
.memory-card__header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; }
.memory-card__header-left { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; min-width: 0; flex: 1; }
.memory-card__id {
  font-family: var(--font-mono); font-size: 12px; color: var(--text-muted);
  background: var(--bg-muted); padding: 2px 8px; border-radius: 4px; flex-shrink: 0;
}
.memory-card__actions { display: flex; gap: 4px; }
.memory-card__content {
  font-size: 13px; line-height: 1.7; color: var(--text-heading); white-space: pre-wrap; word-break: break-all;
  display: -webkit-box; -webkit-line-clamp: 3; -webkit-box-orient: vertical; overflow: hidden;
  cursor: pointer; transition: all 0.2s ease;
}
.memory-card__content.expanded { -webkit-line-clamp: unset; display: block; }
.memory-card__time { font-family: var(--font-mono); font-size: 12px; color: var(--text-dim); flex-shrink: 0; margin-left: auto; }
.memory-card__tags { display: flex; gap: 6px; flex-wrap: wrap; }
.memory-card__check { width: 16px; height: 16px; accent-color: var(--accent); cursor: pointer; }
</style>
