<script lang="ts" setup>
import { useI18n } from 'vue-i18n'

const { t } = useI18n()

defineProps<{
  tags: any[]
  selectedTags: Set<string>
  allSelected: boolean
}>()

const emit = defineEmits<{
  (e: 'select-all'): void
  (e: 'toggle-select', name: string): void
  (e: 'rename', tag: any): void
  (e: 'view', tag: any): void
  (e: 'delete', tag: any): void
}>()
</script>

<template>
  <table class="tag-table">
    <thead>
      <tr>
        <th><input type="checkbox" :checked="allSelected" @change="emit('select-all')" /></th>
        <th>{{ t('tagName') }}</th>
        <th>{{ t('memoryCount') }}</th>
        <th>{{ t('actions') }}</th>
      </tr>
    </thead>
    <tbody>
      <tr v-for="tag in tags" :key="tag.name || tag.tag">
        <td>
          <input
            type="checkbox"
            class="tag-cell__check"
            :checked="selectedTags.has(tag.name || tag.tag)"
            @change="emit('toggle-select', tag.name || tag.tag)"
          />
        </td>
        <td>
          <div class="tag-cell">
            <span class="tag-cell__name">{{ tag.name || tag.tag }}</span>
          </div>
        </td>
        <td><span class="tag-count">{{ tag.count || 0 }}</span></td>
        <td>
          <div class="tag-actions">
            <button class="btn btn--ghost btn--sm" @click="emit('view', tag)">{{ t('view') }}</button>
            <button class="btn btn--ghost btn--sm" @click="emit('rename', tag)">{{ t('rename') }}</button>
            <button class="btn btn--ghost-danger btn--sm" @click="emit('delete', tag)">{{ t('delete') }}</button>
          </div>
        </td>
      </tr>
    </tbody>
  </table>
</template>

<style scoped>
.tag-table { width: 100%; border-collapse: collapse; }
.tag-table th {
  font-family: var(--font-mono); font-size: 11px; font-weight: 600;
  text-transform: uppercase; letter-spacing: 0.5px; color: var(--text-muted);
  text-align: left; padding: 12px 16px; border-bottom: 1px solid var(--border);
}
.tag-table td { padding: 12px 16px; border-bottom: 1px solid var(--border-light); font-size: 13px; color: var(--text-heading); }
.tag-table tr:hover td { background: var(--bg-card-hover); }
.tag-cell { display: flex; align-items: center; gap: 8px; }
.tag-cell__check { width: 16px; height: 16px; accent-color: var(--accent); cursor: pointer; }
.tag-cell__name { background: var(--accent-tag); color: var(--accent-light); padding: 2px 10px; border-radius: var(--radius-pill); font-size: 12px; font-weight: 500; }
.tag-count { font-family: var(--font-mono); font-weight: 600; color: var(--accent); white-space: nowrap; }
.tag-actions { display: flex; gap: 4px; white-space: nowrap; }
.tag-table th:first-child, .tag-table td:first-child { width: 40px; }
.tag-table th:nth-child(3), .tag-table td:nth-child(3) { width: 100px; }
.tag-table th:last-child, .tag-table td:last-child { width: 180px; }
</style>
