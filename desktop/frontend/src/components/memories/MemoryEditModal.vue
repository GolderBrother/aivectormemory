<script lang="ts" setup>
import { ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import Modal from '../layout/Modal.vue'
import { formatTime } from '../../utils/formatTime'

const { t } = useI18n()

const props = defineProps<{
  show: boolean
  memory: any
}>()

const emit = defineEmits<{
  (e: 'close'): void
  (e: 'save', data: { content: string; tags: string[]; scope: string }): void
}>()

const content = ref('')
const tagsInput = ref('')
const scope = ref('project')

watch(() => props.memory, (m) => {
  if (m) {
    content.value = m.content || ''
    tagsInput.value = (m.tags || []).join(', ')
    scope.value = m.scope || 'project'
  }
})

function onSave() {
  emit('save', {
    content: content.value,
    tags: tagsInput.value.split(',').map((s: string) => s.trim()).filter(Boolean),
    scope: scope.value,
  })
}
</script>

<template>
  <Modal :show="show" :title="t('editMemory')" @close="emit('close')" @confirm="onSave">
    <div class="form-field">
      <label class="form-label">{{ t('content') }}</label>
      <textarea v-model="content" class="form-textarea" />
    </div>
    <div class="form-field">
      <label class="form-label">{{ t('tagsCommaSep') }}</label>
      <input v-model="tagsInput" class="form-input" />
    </div>
    <div class="form-field">
      <label class="form-label">{{ t('scope') }}</label>
      <select v-model="scope" class="filter-select">
        <option value="project">{{ t('scopeProject') }}</option>
        <option value="user">{{ t('scopeUser') }}</option>
      </select>
    </div>

    <!-- Detail fields (read-only) -->
    <div v-if="memory" class="detail-fields">
      <div v-if="memory.source" class="detail-row">
        <span class="detail-label">{{ t('source') }}</span>
        <span class="detail-value">{{ memory.source }}</span>
      </div>
      <div v-if="memory.session_id" class="detail-row">
        <span class="detail-label">{{ t('sessionId') }}</span>
        <span class="detail-value mono">{{ memory.session_id }}</span>
      </div>
      <div v-if="memory.similarity" class="detail-row">
        <span class="detail-label">{{ t('similarity') }}</span>
        <span class="detail-value mono">{{ memory.similarity }}</span>
      </div>
      <div v-if="memory.created_at" class="detail-row">
        <span class="detail-label">{{ t('createdAt') }}</span>
        <span class="detail-value mono">{{ formatTime(memory.created_at) }}</span>
      </div>
      <div v-if="memory.updated_at" class="detail-row">
        <span class="detail-label">{{ t('updatedAt') }}</span>
        <span class="detail-value mono">{{ formatTime(memory.updated_at) }}</span>
      </div>
    </div>
  </Modal>
</template>

<style scoped>
.detail-fields { margin-top: 20px; padding-top: 16px; border-top: 1px solid var(--border); }
.detail-row { display: flex; justify-content: space-between; padding: 6px 0; font-size: 12px; }
.detail-label { color: var(--text-muted); font-family: var(--font-mono); font-weight: 600; }
.detail-value { color: var(--text-secondary); }
.detail-value.mono { font-family: var(--font-mono); }
</style>
