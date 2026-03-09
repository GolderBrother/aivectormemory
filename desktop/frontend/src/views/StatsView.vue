<script lang="ts" setup>
import { ref, onMounted, computed } from 'vue'
import { useI18n } from 'vue-i18n'
import { useProjectStore } from '../stores/project'
import { showError } from '../utils/toast'
import { GetStatus, GetTags, GetMemories, GetIssues } from '../../wailsjs/go/main/App'
import BlockAlert from '../components/stats/BlockAlert.vue'
import StatsGrid from '../components/stats/StatsGrid.vue'
import VectorNetwork from '../components/stats/VectorNetwork.vue'
import Modal from '../components/layout/Modal.vue'

const { t } = useI18n()
const projectStore = useProjectStore()

const statusData = ref<any>(null)
const tags = ref<[string, number][]>([])
const showMemoryModal = ref(false)
const memoryModalTitle = ref('')
const memoryModalResults = ref<any[]>([])
const showIssueModal = ref(false)
const issueModalTitle = ref('')
const issueModalResults = ref<any[]>([])

const stats = computed(() => projectStore.stats)
const isBlocked = computed(() => statusData.value?.is_blocked || false)
const blockReason = computed(() => statusData.value?.block_reason || '')
const currentTask = computed(() => statusData.value?.current_task || '')
const totalCount = computed(() => {
  const mem = stats.value?.memories || {}
  const iss = stats.value?.issues || {}
  return (mem.project || 0) + (mem.user || 0) + (iss.pending || 0) + (iss.in_progress || 0) + (iss.completed || 0)
})

onMounted(async () => {
  await projectStore.loadStats()
  try {
    statusData.value = await GetStatus(projectStore.current)
  } catch {}
  try {
    const tagData = await GetTags(projectStore.current, '')
    if (Array.isArray(tagData)) {
      tags.value = tagData.map((t: any) => [t.name || t.tag, t.count || 0] as [string, number])
        .sort((a: [string, number], b: [string, number]) => b[1] - a[1])
    }
  } catch {}
})

async function onCardClick(type: 'memories' | 'issues', scope?: string, status?: string) {
  if (type === 'memories') {
    const scopeVal = scope || 'project'
    memoryModalTitle.value = scopeVal === 'project' ? t('projectMemories') : t('globalMemories')
    try {
      const result = await GetMemories(scopeVal, projectStore.current, '', '', '', 50, 0)
      memoryModalResults.value = result?.memories || []
    } catch (e) {
      memoryModalResults.value = []
      showError(e)
    }
    showMemoryModal.value = true
  } else if (type === 'issues' && status) {
    issueModalTitle.value = t(`status.${status}`)
    try {
      const result = await GetIssues(projectStore.current, status, '', '', 50, 0)
      issueModalResults.value = result?.issues || []
    } catch (e) {
      issueModalResults.value = []
      showError(e)
    }
    showIssueModal.value = true
  }
}

async function onTagClick(tag: string) {
  memoryModalTitle.value = t('tagLabel', { name: tag })
  try {
    const result = await GetMemories('all', projectStore.current, '', tag, '', 50, 0)
    memoryModalResults.value = result?.memories || []
  } catch (e) {
    memoryModalResults.value = []
    showError(e)
  }
  showMemoryModal.value = true
}
</script>

<template>
  <div class="stats-view">
    <BlockAlert :is-blocked="isBlocked" :block-reason="blockReason" :current-task="currentTask" />
    <StatsGrid :memories="stats?.memories" :issues="stats?.issues" @card-click="onCardClick" />

    <VectorNetwork :tags="tags" @tag-click="onTagClick" />

    <div v-if="totalCount === 0" class="welcome-guide">
      <div class="welcome-guide__desc">{{ t('emptyStatsHint') }}</div>
    </div>

    <!-- Memory list modal -->
    <Modal :show="showMemoryModal" :title="memoryModalTitle" hide-footer @close="showMemoryModal = false" width="640px">
      <div v-if="memoryModalResults.length === 0" class="empty-state">{{ t('noMemories') }}</div>
      <ul v-else class="memory-list">
        <li v-for="m in memoryModalResults" :key="m.id" class="memory-item">
          <div class="memory-item__content">{{ m.content }}</div>
          <div class="memory-item__tags">
            <span v-for="tag in (m.tags || [])" :key="tag" class="tag">{{ tag }}</span>
          </div>
        </li>
      </ul>
    </Modal>

    <!-- Issue list modal -->
    <Modal :show="showIssueModal" :title="issueModalTitle" hide-footer @close="showIssueModal = false" width="640px">
      <div v-if="issueModalResults.length === 0" class="empty-state">{{ t('noIssues') }}</div>
      <ul v-else class="issue-list">
        <li v-for="issue in issueModalResults" :key="issue.id" class="issue-item">
          <div class="issue-item__header">
            <span class="issue-item__number">#{{ issue.issue_number }}</span>
            <span class="issue-item__title">{{ issue.title }}</span>
          </div>
          <div v-if="issue.content" class="issue-item__content">{{ issue.content }}</div>
          <div class="issue-item__meta">{{ issue.date }}</div>
        </li>
      </ul>
    </Modal>
  </div>
</template>

<style scoped>
.stats-view { display: flex; flex-direction: column; flex: 1; min-height: 0; }
.welcome-guide { text-align: center; padding: 48px 32px; }
.welcome-guide__desc { font-size: 13px; color: var(--text-muted); line-height: 1.8; }

.memory-list { list-style: none; padding: 0; }
.memory-item {
  padding: 12px 0; border-bottom: 1px solid var(--border-light);
  font-size: 13px; color: var(--text-heading);
}
.memory-item:last-child { border-bottom: none; }
.memory-item__content {
  white-space: pre-wrap; line-height: 1.6; margin-bottom: 8px;
  display: -webkit-box; -webkit-line-clamp: 3; -webkit-box-orient: vertical; overflow: hidden;
}
.memory-item__tags { display: flex; gap: 6px; flex-wrap: wrap; }

.issue-list { list-style: none; padding: 0; }
.issue-item {
  padding: 12px 0; border-bottom: 1px solid var(--border-light);
}
.issue-item:last-child { border-bottom: none; }
.issue-item__header { display: flex; align-items: center; gap: 8px; margin-bottom: 4px; }
.issue-item__number { font-family: var(--font-mono); font-size: 12px; color: var(--text-muted); font-weight: 600; }
.issue-item__title { font-size: 13px; color: var(--text-heading); font-weight: 500; }
.issue-item__content {
  font-size: 12px; color: var(--text-secondary); line-height: 1.5; margin-bottom: 4px;
  display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden;
}
.issue-item__meta { font-size: 11px; color: var(--text-muted); }
</style>
