<script lang="ts" setup>
import { ref, onMounted, computed } from 'vue'
import { useI18n } from 'vue-i18n'
import { useTags } from '../composables/useTags'
import TagTable from '../components/tags/TagTable.vue'
import TagBatchBar from '../components/tags/TagBatchBar.vue'
import SearchBox from '../components/common/SearchBox.vue'
import Modal from '../components/layout/Modal.vue'

const { t } = useI18n()
const {
  tags, loading, selectedTags,
  load, rename, merge, remove, getMemoriesByTag,
  toggleSelect, selectAll, clearSelection, setQuery,
} = useTags()

const allSelected = computed(() => tags.value.length > 0 && selectedTags.value.size === tags.value.length)

// Rename modal
const renameModalShow = ref(false)
const renameTag = ref<any>(null)
const newName = ref('')

// Merge modal
const mergeModalShow = ref(false)
const mergeTarget = ref('')

// Delete modal
const deleteModalShow = ref(false)
const deleteNames = ref<string[]>([])

// View tag memories modal
const viewModalShow = ref(false)
const viewTitle = ref('')
const viewMemories = ref<any[]>([])

onMounted(() => load())

function onSearch(q: string) { setQuery(q) }

// Rename
function openRename(tag: any) {
  renameTag.value = tag
  newName.value = tag.name || tag.tag
  renameModalShow.value = true
}

async function doRename() {
  const old = renameTag.value?.name || renameTag.value?.tag
  if (!old || !newName.value.trim()) return
  try {
    await rename(old, newName.value.trim())
    renameModalShow.value = false
    window.__toast?.show(t('tagRenamed'), 'success')
    load()
  } catch (e: any) {
    window.__toast?.show(e?.message || 'Failed', 'error')
  }
}

// Merge
function openMerge() {
  mergeTarget.value = ''
  mergeModalShow.value = true
}

async function doMerge() {
  if (!mergeTarget.value.trim()) return
  try {
    await merge([...selectedTags.value], mergeTarget.value.trim())
    mergeModalShow.value = false
    clearSelection()
    window.__toast?.show(t('tagsMerged'), 'success')
    load()
  } catch (e: any) {
    window.__toast?.show(e?.message || 'Failed', 'error')
  }
}

// Delete
function openDelete(tag?: any) {
  if (tag) {
    deleteNames.value = [tag.name || tag.tag]
  } else {
    deleteNames.value = [...selectedTags.value]
  }
  deleteModalShow.value = true
}

async function doDelete() {
  try {
    await remove(deleteNames.value)
    deleteModalShow.value = false
    clearSelection()
    window.__toast?.show(t('tagsDeleted'), 'success')
    load()
  } catch (e: any) {
    window.__toast?.show(e?.message || 'Failed', 'error')
  }
}

// View memories
async function openView(tag: any) {
  const name = tag.name || tag.tag
  viewTitle.value = t('tagLabel', { name })
  try {
    viewMemories.value = await getMemoriesByTag(name) || []
  } catch {
    viewMemories.value = []
  }
  viewModalShow.value = true
}
</script>

<template>
  <div class="tags-view">
    <div class="page-header">
      <h2 class="page-title">{{ t('tagManagement') }}</h2>
      <span class="page-count">{{ t('total') }} {{ tags.length }} {{ t('items') }}</span>
    </div>

    <div class="toolbar">
      <SearchBox :placeholder="t('searchTag')" @search="onSearch" />
    </div>

    <TagBatchBar
      :count="selectedTags.size"
      @merge="openMerge"
      @delete="openDelete()"
      @cancel="clearSelection"
    />

    <TagTable
      :tags="tags"
      :selected-tags="selectedTags"
      :all-selected="allSelected"
      @select-all="selectAll"
      @toggle-select="toggleSelect"
      @rename="openRename"
      @view="openView"
      @delete="openDelete"
    />

    <div v-if="!loading && tags.length === 0" class="empty-state">{{ t('noTags') }}</div>

    <!-- Rename modal -->
    <Modal :show="renameModalShow" :title="t('renameTag')" @close="renameModalShow = false" @confirm="doRename">
      <div class="form-field">
        <label class="form-label">{{ t('currentName') }}</label>
        <div style="font-size: 13px; color: var(--text-heading); margin-bottom: 12px">{{ renameTag?.name || renameTag?.tag }}</div>
      </div>
      <div class="form-field">
        <label class="form-label">{{ t('newName') }}</label>
        <input v-model="newName" class="form-input" @keyup.enter="doRename" />
      </div>
    </Modal>

    <!-- Merge modal -->
    <Modal :show="mergeModalShow" :title="t('mergeTags')" @close="mergeModalShow = false" @confirm="doMerge">
      <div class="form-field">
        <label class="form-label">{{ t('mergeFollowing') }}</label>
        <div class="merge-tags-list">
          <span v-for="name in selectedTags" :key="name" class="tag">{{ name }}</span>
        </div>
      </div>
      <div class="form-field">
        <label class="form-label">{{ t('mergeInto') }}</label>
        <input v-model="mergeTarget" class="form-input" @keyup.enter="doMerge" />
      </div>
    </Modal>

    <!-- Delete confirmation -->
    <Modal :show="deleteModalShow" :title="t('delete')" :confirm-text="t('confirm')" danger @close="deleteModalShow = false" @confirm="doDelete">
      <p style="white-space: pre-line">{{ t('confirmDeleteTag', { names: deleteNames.join(', ') }) }}</p>
    </Modal>

    <!-- View memories modal -->
    <Modal :show="viewModalShow" :title="viewTitle" hide-footer @close="viewModalShow = false" width="640px">
      <div v-if="viewMemories.length === 0" class="empty-state">{{ t('noMemories') }}</div>
      <ul v-else class="memory-list">
        <li v-for="m in viewMemories" :key="m.id" class="memory-item">
          <div class="memory-item__content">{{ m.content }}</div>
          <div class="memory-item__tags">
            <span v-for="tag in (m.tags || [])" :key="tag" class="tag">{{ tag }}</span>
          </div>
        </li>
      </ul>
    </Modal>
  </div>
</template>

<style scoped>
.tags-view { display: flex; flex-direction: column; flex: 1; }
.merge-tags-list { display: flex; gap: 6px; flex-wrap: wrap; margin-bottom: 12px; }
.memory-list { list-style: none; padding: 0; }
.memory-item { padding: 12px 0; border-bottom: 1px solid var(--border-light); font-size: 13px; color: var(--text-heading); }
.memory-item:last-child { border-bottom: none; }
.memory-item__content { white-space: pre-wrap; line-height: 1.6; margin-bottom: 8px; display: -webkit-box; -webkit-line-clamp: 3; -webkit-box-orient: vertical; overflow: hidden; }
.memory-item__tags { display: flex; gap: 6px; flex-wrap: wrap; }
</style>
