<script lang="ts" setup>
import { ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import Modal from '../layout/Modal.vue'

const { t } = useI18n()

const props = defineProps<{
  show: boolean
  issue?: any
  mode?: 'create' | 'edit'
}>()

const emit = defineEmits<{
  (e: 'close'): void
  (e: 'save', data: any): void
}>()

const title = ref('')
const content = ref('')
const status = ref('pending')
const tagsInput = ref('')
const showExtra = ref(false)

// Extra fields
const description = ref('')
const investigation = ref('')
const rootCause = ref('')
const solution = ref('')
const filesChanged = ref('')
const testResult = ref('')
const notes = ref('')
const featureId = ref('')

watch(() => props.issue, (m) => {
  if (m) {
    title.value = m.title || ''
    content.value = m.content || ''
    status.value = m.status || 'pending'
    tagsInput.value = (m.tags || []).join(', ')
    description.value = m.description || ''
    investigation.value = m.investigation || ''
    rootCause.value = m.root_cause || ''
    solution.value = m.solution || ''
    filesChanged.value = Array.isArray(m.files_changed) ? m.files_changed.join('\n') : (m.files_changed || '')
    testResult.value = m.test_result || ''
    notes.value = m.notes || ''
    featureId.value = m.feature_id || ''
    showExtra.value = false
  }
})

watch(() => props.show, (v) => {
  if (v && props.mode === 'create') {
    title.value = ''; content.value = ''; status.value = 'pending'; tagsInput.value = ''
    description.value = ''; investigation.value = ''; rootCause.value = ''
    solution.value = ''; filesChanged.value = ''; testResult.value = ''
    notes.value = ''; featureId.value = ''; showExtra.value = false
  }
})

function onSave() {
  const tags = tagsInput.value.split(',').map(s => s.trim()).filter(Boolean)
  const files = filesChanged.value.split('\n').map(s => s.trim()).filter(Boolean)
  emit('save', {
    title: title.value,
    content: content.value,
    status: status.value,
    tags,
    description: description.value,
    investigation: investigation.value,
    root_cause: rootCause.value,
    solution: solution.value,
    files_changed: files,
    test_result: testResult.value,
    notes: notes.value,
    feature_id: featureId.value,
  })
}
</script>

<template>
  <Modal :show="show" :title="mode === 'create' ? t('addIssue') : t('editIssue')" @close="emit('close')" @confirm="onSave">
    <div class="form-field">
      <label class="form-label">{{ t('issueTitle') }}</label>
      <input v-model="title" class="form-input" />
    </div>
    <div v-if="mode !== 'create'" class="form-field">
      <label class="form-label">{{ t('allStatus') }}</label>
      <select v-model="status" class="filter-select">
        <option value="pending">{{ t('status.pending') }}</option>
        <option value="in_progress">{{ t('status.in_progress') }}</option>
        <option value="completed">{{ t('status.completed') }}</option>
      </select>
    </div>
    <div class="form-field">
      <label class="form-label">{{ t('issueContent') }}</label>
      <textarea v-model="content" class="form-textarea" style="min-height: 120px" />
    </div>
    <div class="form-field">
      <label class="form-label">{{ t('issueTags') }}</label>
      <input v-model="tagsInput" class="form-input" />
    </div>

    <!-- Extra fields toggle -->
    <details class="issue-extra-fields" :open="showExtra" @toggle="showExtra = !showExtra">
      <summary class="issue-extra-toggle">{{ t('moreFields') }}</summary>
      <div class="form-field">
        <label class="form-label">{{ t('issueDescription') }}</label>
        <textarea v-model="description" class="form-textarea" style="min-height: 80px" />
      </div>
      <div class="form-field">
        <label class="form-label">{{ t('issueInvestigation') }}</label>
        <textarea v-model="investigation" class="form-textarea" style="min-height: 80px" />
      </div>
      <div class="form-field">
        <label class="form-label">{{ t('issueRootCause') }}</label>
        <textarea v-model="rootCause" class="form-textarea" style="min-height: 60px" />
      </div>
      <div class="form-field">
        <label class="form-label">{{ t('issueSolution') }}</label>
        <textarea v-model="solution" class="form-textarea" style="min-height: 80px" />
      </div>
      <div class="form-field">
        <label class="form-label">{{ t('issueFilesChanged') }}</label>
        <textarea v-model="filesChanged" class="form-textarea" style="min-height: 60px" placeholder="one file per line" />
      </div>
      <div class="form-field">
        <label class="form-label">{{ t('issueTestResult') }}</label>
        <input v-model="testResult" class="form-input" />
      </div>
      <div class="form-field">
        <label class="form-label">{{ t('issueNotes') }}</label>
        <textarea v-model="notes" class="form-textarea" style="min-height: 60px" />
      </div>
      <div class="form-field">
        <label class="form-label">{{ t('issueFeatureId') }}</label>
        <input v-model="featureId" class="form-input" />
      </div>
    </details>
  </Modal>
</template>

<style scoped>
.issue-extra-fields { margin-top: 12px; border: 1px solid var(--border); border-radius: var(--radius); padding: 0; }
.issue-extra-fields[open] { padding: 0 12px 12px; }
.issue-extra-toggle {
  cursor: pointer; padding: 10px 12px; font-size: 13px; color: var(--text-secondary);
  font-weight: 500; list-style: none; user-select: none;
}
.issue-extra-toggle::-webkit-details-marker { display: none; }
.issue-extra-toggle::before { content: '\25B6 '; font-size: 10px; transition: transform 0.2s; display: inline-block; }
.issue-extra-fields[open] > .issue-extra-toggle::before { transform: rotate(90deg); }
.issue-extra-fields[open] > .issue-extra-toggle { margin-bottom: 8px; color: var(--text-heading); }
</style>
