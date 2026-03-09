<script lang="ts" setup>
import { ref } from 'vue'
import { useI18n } from 'vue-i18n'
import Badge from '../common/Badge.vue'
import { formatTime } from '../../utils/formatTime'

const { t } = useI18n()

const props = defineProps<{
  issue: any
}>()

const emit = defineEmits<{
  (e: 'edit', issue: any): void
  (e: 'archive', issue: any): void
  (e: 'delete', issue: any): void
  (e: 'view', issue: any): void
}>()

const expanded = ref(false)

const statusType: Record<string, 'warning' | 'info' | 'success' | 'muted'> = {
  pending: 'warning',
  in_progress: 'info',
  completed: 'success',
  archived: 'muted',
}

const structuredFields = ['description', 'investigation', 'root_cause', 'solution', 'files_changed', 'test_result', 'notes']
const fieldLabels: Record<string, string> = {
  description: 'issueDescription',
  investigation: 'issueInvestigation',
  root_cause: 'issueRootCause',
  solution: 'issueSolution',
  files_changed: 'issueFilesChanged',
  test_result: 'issueTestResult',
  notes: 'issueNotes',
}

function hasStructured(): boolean {
  return structuredFields.some(f => props.issue[f])
}

function toggleExpand() {
  expanded.value = !expanded.value
}
</script>

<template>
  <div :class="['issue-card', expanded && 'expanded']" @click="toggleExpand">
    <div class="issue-card__header">
      <div class="issue-card__title">
        <span class="issue-card__number">{{ t('issuePrefix') }}{{ issue.issue_number }}</span>
        {{ issue.title }}
        <Badge :type="(statusType[issue.status] || 'muted') as any">{{ t(`status.${issue.status}`) }}</Badge>
        <span v-if="issue.parent_id" class="issue-parent-tag">{{ t('relatedTo') }} #{{ issue.parent_id }}</span>
        <span v-if="issue.task_done != null && issue.task_total" class="issue-task-progress">
          {{ t('taskProgress', { done: issue.task_done, total: issue.task_total }) }}
        </span>
      </div>
      <div class="issue-card__actions" @click.stop>
        <template v-if="issue.status !== 'archived'">
          <button class="btn btn--ghost btn--sm" @click="emit('edit', issue)">{{ t('edit') }}</button>
          <button class="btn btn--ghost btn--sm" @click="emit('archive', issue)">{{ t('archiveIssue') }}</button>
        </template>
        <template v-else>
          <button class="btn btn--ghost btn--sm" @click="emit('view', issue)">{{ t('view') }}</button>
        </template>
        <button class="btn btn--ghost-danger btn--sm" @click="emit('delete', issue)">{{ t('delete') }}</button>
      </div>
    </div>

    <div v-if="issue.content" class="issue-card__content">{{ issue.content }}</div>

    <!-- Structured fields -->
    <div v-if="hasStructured()" class="issue-structured">
      <template v-for="field in structuredFields" :key="field">
        <div v-if="issue[field]" class="issue-field">
          <span class="issue-field__label">{{ t(fieldLabels[field]) }}</span>
          <div class="issue-field__value">{{ typeof issue[field] === 'string' ? issue[field] : JSON.stringify(issue[field]) }}</div>
        </div>
      </template>
    </div>

    <div v-if="issue.tags?.length" class="issue-card__tags">
      <span v-for="tag in issue.tags" :key="tag" class="tag">{{ tag }}</span>
    </div>

    <div class="issue-card__meta">{{ formatTime(issue.created_at) }}</div>
    <div v-if="hasStructured() || issue.content" class="issue-card__expand">
      {{ expanded ? t('collapseDetails') : t('expandDetails') }}
    </div>
  </div>
</template>

<style scoped>
.issue-card {
  background: var(--bg-surface); border-radius: var(--radius-md); padding: 18px 20px;
  border: 1px solid var(--border); transition: all 0.15s ease; cursor: pointer;
}
.issue-card:hover { border-color: var(--border-hover); box-shadow: var(--shadow-card); }
.issue-card__header { display: flex; justify-content: space-between; align-items: flex-start; }
.issue-card__title { font-size: 14px; font-weight: 600; color: var(--text-primary); display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
.issue-card__number { font-family: var(--font-mono); color: var(--text-muted); font-weight: 500; font-size: 13px; }
.issue-card__content {
  font-size: 13px; line-height: 1.6; color: var(--text-secondary); margin-top: 10px;
  white-space: pre-wrap; word-break: break-all;
  max-height: 0; overflow: hidden; transition: max-height 0.3s ease, opacity 0.3s ease; opacity: 0;
}
.issue-card.expanded .issue-card__content { max-height: 500px; opacity: 1; }
.issue-card__meta { font-family: var(--font-mono); font-size: 12px; color: var(--text-dim); margin-top: 10px; }
.issue-card__expand { font-size: 11px; color: var(--text-muted); margin-top: 6px; transition: color 0.15s; }
.issue-card:hover .issue-card__expand { color: var(--text-secondary); }
.issue-card__actions { display: flex; gap: 4px; flex-shrink: 0; }
.issue-card__actions .btn { opacity: 0; transition: opacity 0.15s ease; }
.issue-card:hover .issue-card__actions .btn { opacity: 1; }
.issue-card__tags { display: flex; gap: 6px; flex-wrap: wrap; margin-top: 8px; }

.issue-parent-tag {
  display: inline-block; margin-left: 8px; padding: 1px 6px; border-radius: 4px;
  background: var(--color-purple-bg); color: var(--color-purple); font-size: 11px; font-weight: 500;
}
.issue-task-progress {
  display: inline-flex; align-items: center; margin-left: 8px; padding: 2px 8px;
  border-radius: 10px; font-size: 12px; font-weight: 500;
  background: var(--accent-tag); color: var(--accent);
}

.issue-structured { max-height: 0; overflow: hidden; opacity: 0; transition: max-height 0.3s, opacity 0.2s; }
.issue-card.expanded .issue-structured { max-height: 2000px; opacity: 1; margin-top: 10px; }
.issue-field {
  margin-bottom: 8px; padding: 6px 10px; background: var(--bg-muted);
  border-radius: 6px; border-left: 3px solid var(--border);
}
.issue-field__label {
  display: block; font-size: 11px; font-weight: 600; color: var(--text-secondary);
  margin-bottom: 3px; text-transform: uppercase; letter-spacing: 0.5px;
}
.issue-field__value { font-size: 13px; color: var(--text-heading); white-space: pre-wrap; word-break: break-word; }
</style>
