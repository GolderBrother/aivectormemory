<script lang="ts" setup>
import { ref, onMounted } from 'vue'
import { useI18n } from 'vue-i18n'
import { useProjectStore } from '../stores/project'
import { showError } from '../utils/toast'
import { GetStatus, UpdateStatus } from '../../wailsjs/go/main/App'
import Badge from '../components/common/Badge.vue'
import { formatTime } from '../utils/formatTime'

const { t } = useI18n()
const projectStore = useProjectStore()

const status = ref<any>(null)
const loading = ref(true)

// Inline edit state
const editingField = ref('')
const editValue = ref('')

onMounted(async () => {
  await loadStatus()
})

async function loadStatus() {
  loading.value = true
  try {
    status.value = await GetStatus(projectStore.current)
  } catch {
    status.value = null
  } finally {
    loading.value = false
  }
}

function startEdit(field: string, current: any) {
  editingField.value = field
  editValue.value = typeof current === 'boolean' ? String(current) : (current || '')
}

async function saveEdit(field: string) {
  if (!status.value) return
  try {
    const val = field === 'is_blocked' ? editValue.value : editValue.value
    await UpdateStatus(projectStore.current, field, [val])
    editingField.value = ''
    window.__toast?.show(t('opSuccess'), 'success')
    await loadStatus()
  } catch (e: any) {
    window.__toast?.show(e?.message || 'Failed', 'error')
  }
}

function cancelEdit() { editingField.value = '' }

async function toggleBlocked() {
  if (!status.value) return
  const newVal = !status.value.is_blocked
  try {
    await UpdateStatus(projectStore.current, 'is_blocked', [String(newVal)])
    await loadStatus()
  } catch (e) { showError(e) }
}

// List field management
async function addListItem(field: string) {
  if (!editValue.value.trim()) return
  const current = status.value?.[field] || []
  const updated = [...current, editValue.value.trim()]
  try {
    await UpdateStatus(projectStore.current, field, updated)
    editValue.value = ''
    await loadStatus()
  } catch (e) { showError(e) }
}

async function removeListItem(field: string, index: number) {
  const current = [...(status.value?.[field] || [])]
  current.splice(index, 1)
  try {
    await UpdateStatus(projectStore.current, field, current)
    await loadStatus()
  } catch (e) { showError(e) }
}
</script>

<template>
  <div class="status-view">
    <div class="page-header">
      <h2 class="page-title">{{ t('sessionStatus') }}</h2>
    </div>

    <div v-if="loading" class="empty-state">Loading...</div>
    <div v-else-if="!status" class="empty-state">{{ t('noStatus') }}</div>
    <template v-else>
      <!-- Status grid -->
      <div class="status-grid">
        <!-- Blocked -->
        <div class="status-field">
          <div class="status-field__label">{{ t('blocked') }}</div>
          <div class="status-field__value">
            <Badge :type="status.is_blocked ? 'warning' : 'success'">
              {{ status.is_blocked ? t('yes') : t('no') }}
            </Badge>
            <button class="btn btn--ghost btn--sm" @click="toggleBlocked">
              {{ status.is_blocked ? 'Unblock' : 'Block' }}
            </button>
          </div>
        </div>

        <!-- Block reason -->
        <div v-if="status.is_blocked" class="status-field">
          <div class="status-field__label">block_reason</div>
          <div class="status-field__value">
            <template v-if="editingField === 'block_reason'">
              <input v-model="editValue" class="form-input" @keyup.enter="saveEdit('block_reason')" @keyup.escape="cancelEdit" />
              <button class="btn btn--primary btn--sm" @click="saveEdit('block_reason')">{{ t('save') }}</button>
            </template>
            <template v-else>
              <span>{{ status.block_reason || '-' }}</span>
              <button class="btn btn--ghost btn--sm" @click="startEdit('block_reason', status.block_reason)">{{ t('edit') }}</button>
            </template>
          </div>
        </div>

        <!-- Current task -->
        <div class="status-field">
          <div class="status-field__label">{{ t('currentTask') }}</div>
          <div class="status-field__value">
            <template v-if="editingField === 'current_task'">
              <input v-model="editValue" class="form-input" @keyup.enter="saveEdit('current_task')" @keyup.escape="cancelEdit" />
              <button class="btn btn--primary btn--sm" @click="saveEdit('current_task')">{{ t('save') }}</button>
            </template>
            <template v-else>
              <span>{{ status.current_task || '-' }}</span>
              <button class="btn btn--ghost btn--sm" @click="startEdit('current_task', status.current_task)">{{ t('edit') }}</button>
            </template>
          </div>
        </div>

        <!-- Next step -->
        <div class="status-field">
          <div class="status-field__label">{{ t('nextStep') }}</div>
          <div class="status-field__value">
            <template v-if="editingField === 'next_step'">
              <input v-model="editValue" class="form-input" @keyup.enter="saveEdit('next_step')" @keyup.escape="cancelEdit" />
              <button class="btn btn--primary btn--sm" @click="saveEdit('next_step')">{{ t('save') }}</button>
            </template>
            <template v-else>
              <span>{{ status.next_step || '-' }}</span>
              <button class="btn btn--ghost btn--sm" @click="startEdit('next_step', status.next_step)">{{ t('edit') }}</button>
            </template>
          </div>
        </div>

        <!-- Updated at -->
        <div class="status-field">
          <div class="status-field__label">{{ t('updateTime') }}</div>
          <div class="status-field__value">{{ status.updated_at ? formatTime(status.updated_at) : '-' }}</div>
        </div>
      </div>

      <!-- List sections -->
      <div v-for="listField in ['progress', 'recent_changes', 'pending']" :key="listField" class="status-list-section">
        <h3 class="status-list-title">{{ t(listField === 'recent_changes' ? 'recentChanges' : listField) }}</h3>
        <ul v-if="status[listField]?.length" class="status-list">
          <li v-for="(item, idx) in status[listField]" :key="idx">
            <span>{{ item }}</span>
            <button class="btn btn--ghost-danger btn--sm" @click="removeListItem(listField, idx)">×</button>
          </li>
        </ul>
        <div v-else class="empty-state" style="padding: 12px">-</div>
        <div class="status-list-add">
          <input
            v-model="editValue"
            class="form-input"
            :placeholder="`Add ${listField} item...`"
            @keyup.enter="addListItem(listField)"
            @focus="editingField = listField"
          />
          <button class="btn btn--outline btn--sm" @click="addListItem(listField)">+</button>
        </div>
      </div>
    </template>
  </div>
</template>

<style scoped>
.status-view { display: flex; flex-direction: column; flex: 1; }
.status-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 24px; }
.status-field {
  background: var(--bg-primary); border-radius: var(--radius-sm); padding: 12px;
  
}
.status-field__label {
  font-size: 11px; font-weight: 500;
  color: var(--text-muted); margin-bottom: 2px;
}
.status-field__value {
  display: flex; align-items: center; gap: 8px; font-size: 14px; color: var(--text-primary);
}
.status-field__value .form-input { flex: 1; }

.status-list-section { margin-bottom: 20px; }
.status-list-title {
  font-family: var(--font-mono); font-size: 12px; font-weight: 600;
  color: var(--text-muted); margin-bottom: 2px;
}
.status-list { list-style: none; padding: 0; }
.status-list li {
  display: flex; justify-content: space-between; align-items: center;
  padding: 8px 12px; font-size: 13px; color: var(--text-heading);
  background: var(--bg-surface);  border-radius: var(--radius);
  margin-bottom: 4px;
}
.status-list-add { display: flex; gap: 8px; margin-top: 8px; }
.status-list-add .form-input { flex: 1; }
</style>
