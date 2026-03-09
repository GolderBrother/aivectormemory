<script lang="ts" setup>
import { ref, onMounted, computed } from 'vue'
import { useI18n } from 'vue-i18n'
import { useMemories } from '../composables/useMemories'
import { showError } from '../utils/toast'
import MemoryCard from '../components/memories/MemoryCard.vue'
import MemoryEditModal from '../components/memories/MemoryEditModal.vue'
import SearchBox from '../components/common/SearchBox.vue'
import Pager from '../components/common/Pager.vue'
import Modal from '../components/layout/Modal.vue'

const { t } = useI18n()
const props = defineProps<{ scope?: string }>()

const effectiveScope = computed(() => props.scope || 'project')
const {
  memories, total, page, loading,
  load, getDetail, update, remove, batchDelete,
  exportData, importData, setPage, setQuery, PAGE_SIZE,
} = useMemories(effectiveScope.value)

// Edit modal
const editModalShow = ref(false)
const editMemory = ref<any>(null)

// Batch select
const batchMode = ref(false)
const selectedIds = ref<Set<string>>(new Set())

// Import
const fileInput = ref<HTMLInputElement | null>(null)

onMounted(() => load())

async function onEdit(id: string) {
  try {
    editMemory.value = await getDetail(id)
    editModalShow.value = true
  } catch (e) { showError(e) }
}

async function onSave(data: { content: string; tags: string[]; scope: string }) {
  if (!editMemory.value) return
  try {
    await update(editMemory.value.id, data.content, data.tags, data.scope)
    editModalShow.value = false
    window.__toast?.show(t('memorySaved'), 'success')
    load()
  } catch (e: any) {
    window.__toast?.show(e?.message || 'Failed', 'error')
  }
}

async function onDelete(id: string) {
  if (!confirm(t('confirmDelete'))) return
  try {
    await remove(id)
    window.__toast?.show(t('memoryDeleted'), 'success')
    load()
  } catch (e: any) {
    window.__toast?.show(e?.message || 'Failed', 'error')
  }
}

// Batch
function toggleSelect(id: string) {
  const s = new Set(selectedIds.value)
  s.has(id) ? s.delete(id) : s.add(id)
  selectedIds.value = s
}

function selectAll() {
  if (selectedIds.value.size === memories.value.length) {
    selectedIds.value = new Set()
  } else {
    selectedIds.value = new Set(memories.value.map((m: any) => m.id))
  }
}

async function doBatchDelete() {
  const ids = [...selectedIds.value]
  if (!ids.length) return
  if (!confirm(t('confirmBatchDelete', { n: ids.length }))) return
  try {
    await batchDelete(ids)
    window.__toast?.show(t('memoriesDeleted', { n: ids.length }), 'success')
    selectedIds.value = new Set()
    batchMode.value = false
    load()
  } catch (e: any) {
    window.__toast?.show(e?.message || 'Failed', 'error')
  }
}

// Export
async function doExport() {
  try {
    const data = await exportData(effectiveScope.value)
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' })
    const a = document.createElement('a')
    a.href = URL.createObjectURL(blob)
    a.download = `aivectormemory-export-${new Date().toISOString().slice(0, 10)}.json`
    a.click()
    URL.revokeObjectURL(a.href)
    window.__toast?.show(t('exportSuccess'), 'success')
  } catch (e: any) {
    window.__toast?.show(e?.message || 'Failed', 'error')
  }
}

// Import
function triggerImport() {
  fileInput.value?.click()
}

async function onFileSelected(e: Event) {
  const file = (e.target as HTMLInputElement).files?.[0]
  if (!file) return
  try {
    const text = await file.text()
    const result = await importData(text)
    const n = result?.imported || result?.count || 0
    window.__toast?.show(t('importSuccess', { n }), 'success')
    load()
  } catch (e: any) {
    window.__toast?.show(t('importFailed') + ': ' + (e?.message || ''), 'error')
  }
  if (fileInput.value) fileInput.value.value = ''
}

function onSearch(q: string) {
  setQuery(q)
}
</script>

<template>
  <div class="memories-view">
    <div class="page-header">
      <h2 class="page-title">{{ effectiveScope === 'user' ? t('globalMemories') : t('projectMemories') }}</h2>
      <span class="page-count">{{ t('total') }} {{ total }} {{ t('items') }}</span>
    </div>

    <!-- Toolbar -->
    <div class="toolbar toolbar--wrap">
      <SearchBox
        :placeholder="effectiveScope === 'user' ? t('searchGlobal') : t('searchProject')"
        @search="onSearch"
      />
      <div class="toolbar-right">
        <button class="btn btn--outline btn--sm" @click="doExport">{{ t('exportBtn') }}</button>
        <button class="btn btn--outline btn--sm" @click="triggerImport">{{ t('importBtn') }}</button>
        <button
          class="btn btn--sm"
          :class="batchMode ? 'btn--primary' : 'btn--outline'"
          @click="batchMode = !batchMode; selectedIds = new Set()"
        >{{ batchMode ? t('cancelSelect') : t('batchDeleteMemories') }}</button>
      </div>
      <input ref="fileInput" type="file" accept=".json" style="display:none" @change="onFileSelected" />
    </div>

    <!-- Batch bar -->
    <div v-if="batchMode" class="batch-bar">
      <label class="select-all">
        <input type="checkbox" :checked="selectedIds.size === memories.length && memories.length > 0" @change="selectAll" />
        {{ t('selectAll') }}
      </label>
      <span class="batch-bar__info">{{ t('selected') }} <strong>{{ selectedIds.size }}</strong> {{ t('items') }}</span>
      <button class="btn btn--danger btn--sm" :disabled="!selectedIds.size" @click="doBatchDelete">{{ t('batchDelete') }}</button>
    </div>

    <!-- Memory list -->
    <div class="card-list">
      <MemoryCard
        v-for="m in memories"
        :key="m.id"
        :memory="m"
        :selectable="batchMode"
        :selected="selectedIds.has(m.id)"
        @edit="onEdit"
        @delete="onDelete"
        @toggle-select="toggleSelect"
      />
      <div v-if="!loading && memories.length === 0" class="empty-state">{{ t('noMemories') }}</div>
    </div>

    <!-- Pager -->
    <Pager :page="page" :total="total" :page-size="PAGE_SIZE" @page-change="setPage" />

    <!-- Edit modal -->
    <MemoryEditModal
      :show="editModalShow"
      :memory="editMemory"
      @close="editModalShow = false"
      @save="onSave"
    />
  </div>
</template>

<style scoped>
.memories-view { display: flex; flex-direction: column; flex: 1; }
.batch-bar {
  display: flex; align-items: center; gap: 12px; padding: 12px 16px;
  background: var(--bg-surface); border-radius: var(--radius-md);
  border: 1px solid var(--border); margin-bottom: 16px;
}
.batch-bar__info { font-size: 13px; color: var(--text-secondary); }
.batch-bar__info strong { color: var(--text-primary); }
.select-all { display: flex; align-items: center; gap: 8px; font-size: 12px; color: var(--text-secondary); cursor: pointer; }
.select-all input { accent-color: var(--accent); }
</style>
