<script lang="ts" setup>
import { ref, onMounted, computed } from 'vue'
import { useI18n } from 'vue-i18n'
import { useTasks } from '../composables/useTasks'
import TaskGroup from '../components/tasks/TaskGroup.vue'
import SearchBox from '../components/common/SearchBox.vue'
import Modal from '../components/layout/Modal.vue'

const { t } = useI18n()
const {
  tasks, loading, collapsedGroups,
  load, createGroup, update, remove, removeFeature,
  toggleGroup, setFeature, setStatus, setQuery,
} = useTasks()

// Modals
const createModalShow = ref(false)
const newFeatureId = ref('')
const newTaskTitle = ref('')

const addTaskModalShow = ref(false)
const addTaskFeature = ref('')
const addTaskTitle = ref('')

const editModalShow = ref(false)
const editTask = ref<any>(null)
const editTitle = ref('')

const confirmModalShow = ref(false)
const confirmType = ref<'task' | 'group'>('task')
const confirmTarget = ref<any>(null)

// Feature list for filter
const features = computed(() => {
  const s = new Set<string>()
  tasks.value.forEach((g: any) => { if (g.feature_id) s.add(g.feature_id) })
  return [...s]
})

onMounted(() => load())

function onSearch(q: string) { setQuery(q) }
function onStatusChange(e: Event) { setStatus((e.target as HTMLSelectElement).value) }
function onFeatureChange(e: Event) { setFeature((e.target as HTMLSelectElement).value) }

// Create group
function openCreateGroup() {
  newFeatureId.value = ''
  newTaskTitle.value = ''
  createModalShow.value = true
}

async function doCreateGroup() {
  if (!newFeatureId.value.trim()) return
  try {
    await createGroup(newFeatureId.value.trim(), newTaskTitle.value.trim())
    createModalShow.value = false
    window.__toast?.show(t('featureGroupCreated'), 'success')
    load()
  } catch (e: any) {
    window.__toast?.show(e?.message || 'Failed', 'error')
  }
}

// Add task to group
function openAddTask(featureId: string) {
  addTaskFeature.value = featureId
  addTaskTitle.value = ''
  addTaskModalShow.value = true
}

async function doAddTask() {
  if (!addTaskTitle.value.trim()) return
  try {
    await createGroup(addTaskFeature.value, addTaskTitle.value.trim())
    addTaskModalShow.value = false
    window.__toast?.show(t('taskCreated'), 'success')
    load()
  } catch (e: any) {
    window.__toast?.show(e?.message || 'Failed', 'error')
  }
}

// Toggle task status
async function onToggleTask(task: any) {
  const next = task.status === 'done' || task.status === 'completed' ? 'pending' : 'done'
  try {
    await update(task.id, 'status', next)
    load()
  } catch {}
}

// Edit task
function openEditTask(task: any) {
  editTask.value = task
  editTitle.value = task.title
  editModalShow.value = true
}

async function doEditTask() {
  if (!editTask.value || !editTitle.value.trim()) return
  try {
    await update(editTask.value.id, 'title', editTitle.value.trim())
    editModalShow.value = false
    window.__toast?.show(t('taskUpdated'), 'success')
    load()
  } catch (e: any) {
    window.__toast?.show(e?.message || 'Failed', 'error')
  }
}

// Delete
function openDeleteTask(task: any) {
  confirmTarget.value = task
  confirmType.value = 'task'
  confirmModalShow.value = true
}

function openDeleteGroup(group: any) {
  confirmTarget.value = group
  confirmType.value = 'group'
  confirmModalShow.value = true
}

async function doDelete() {
  try {
    if (confirmType.value === 'task') {
      await remove(confirmTarget.value.id)
      window.__toast?.show(t('taskDeleted'), 'success')
    } else {
      await removeFeature(confirmTarget.value.feature_id)
      window.__toast?.show(t('featureGroupDeleted'), 'success')
    }
    confirmModalShow.value = false
    load()
  } catch (e: any) {
    window.__toast?.show(e?.message || 'Failed', 'error')
  }
}
</script>

<template>
  <div class="tasks-view">
    <div class="page-header">
      <h2 class="page-title">{{ t('taskManagement') }}</h2>
    </div>

    <div class="toolbar toolbar--wrap">
      <select class="filter-select" @change="onFeatureChange">
        <option value="">{{ t('allFeatures') }}</option>
        <option v-for="f in features" :key="f" :value="f">{{ f }}</option>
      </select>
      <select class="filter-select" @change="onStatusChange">
        <option value="">{{ t('allStatus') }}</option>
        <option value="pending">{{ t('status.pending') }}</option>
        <option value="in_progress">{{ t('status.in_progress') }}</option>
        <option value="completed">{{ t('status.completed') }}</option>
      </select>
      <SearchBox :placeholder="t('searchTask')" @search="onSearch" />
      <div class="toolbar-right">
        <button class="btn btn--primary btn--sm" @click="openCreateGroup">{{ t('addFeatureGroup') }}</button>
      </div>
    </div>

    <div class="task-groups">
      <TaskGroup
        v-for="group in tasks"
        :key="group.feature_id"
        :group="group"
        :collapsed="collapsedGroups.has(group.feature_id)"
        @toggle-collapse="toggleGroup(group.feature_id)"
        @add-task="openAddTask(group.feature_id)"
        @delete-group="openDeleteGroup(group)"
        @toggle-task="onToggleTask"
        @edit-task="openEditTask"
        @delete-task="openDeleteTask"
      />
      <div v-if="!loading && tasks.length === 0" class="empty-state">{{ t('noTasks') }}</div>
    </div>

    <!-- Create group modal -->
    <Modal :show="createModalShow" :title="t('addFeatureGroup')" @close="createModalShow = false" @confirm="doCreateGroup">
      <div class="form-field">
        <label class="form-label">{{ t('featureGroupName') }}</label>
        <input v-model="newFeatureId" class="form-input" @keyup.enter="doCreateGroup" />
      </div>
      <div class="form-field">
        <label class="form-label">{{ t('taskTitle') }}</label>
        <input v-model="newTaskTitle" class="form-input" />
      </div>
    </Modal>

    <!-- Add task modal -->
    <Modal :show="addTaskModalShow" :title="t('addTask')" @close="addTaskModalShow = false" @confirm="doAddTask">
      <div class="form-field">
        <label class="form-label">{{ t('taskTitle') }}</label>
        <input v-model="addTaskTitle" class="form-input" @keyup.enter="doAddTask" />
      </div>
    </Modal>

    <!-- Edit task modal -->
    <Modal :show="editModalShow" :title="t('editTask')" @close="editModalShow = false" @confirm="doEditTask">
      <div class="form-field">
        <label class="form-label">{{ t('taskTitle') }}</label>
        <input v-model="editTitle" class="form-input" @keyup.enter="doEditTask" />
      </div>
    </Modal>

    <!-- Delete confirmation -->
    <Modal
      :show="confirmModalShow"
      :title="confirmType === 'task' ? t('deleteTask') : t('deleteFeatureGroup')"
      :confirm-text="t('confirm')"
      danger
      @close="confirmModalShow = false"
      @confirm="doDelete"
    >
      <p>{{ confirmType === 'task' ? t('confirmDeleteTask') : t('confirmDeleteFeatureGroup') }}</p>
    </Modal>
  </div>
</template>

<style scoped>
.tasks-view { display: flex; flex-direction: column; flex: 1; }
.task-groups { display: flex; flex-direction: column; gap: 16px; }
</style>
