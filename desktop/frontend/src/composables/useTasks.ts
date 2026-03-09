import { ref } from 'vue'
import {
  GetTasks, GetArchivedTasks, CreateTasks, UpdateTask,
  DeleteTask, DeleteTasksByFeature,
} from '../../wailsjs/go/main/App'
import { useProjectStore } from '../stores/project'

export function useTasks() {
  const projectStore = useProjectStore()
  const tasks = ref<any[]>([])
  const archivedTasks = ref<any[]>([])
  const loading = ref(false)
  const featureFilter = ref('')
  const statusFilter = ref('')
  const query = ref('')
  const collapsedGroups = ref<Set<string>>(new Set())

  async function load() {
    loading.value = true
    try {
      const data = await GetTasks(projectStore.current, featureFilter.value, statusFilter.value, query.value)
      tasks.value = data || []
    } catch {
      tasks.value = []
    } finally {
      loading.value = false
    }
  }

  async function loadArchived(featureId: string) {
    try {
      archivedTasks.value = await GetArchivedTasks(projectStore.current, featureId) || []
    } catch {
      archivedTasks.value = []
    }
  }

  async function createGroup(featureId: string, title: string) {
    return await CreateTasks(projectStore.current, featureId, title, '')
  }

  async function update(id: number, field: string, value: string) {
    return await UpdateTask(id, field, value)
  }

  async function remove(id: number) {
    await DeleteTask(id, projectStore.current)
  }

  async function removeFeature(featureId: string) {
    return await DeleteTasksByFeature(featureId, projectStore.current)
  }

  function toggleGroup(featureId: string) {
    const s = new Set(collapsedGroups.value)
    s.has(featureId) ? s.delete(featureId) : s.add(featureId)
    collapsedGroups.value = s
  }

  function setFeature(f: string) { featureFilter.value = f; load() }
  function setStatus(s: string) { statusFilter.value = s; load() }
  function setQuery(q: string) { query.value = q; load() }

  return {
    tasks, archivedTasks, loading, featureFilter, statusFilter, query,
    collapsedGroups, load, loadArchived, createGroup, update, remove,
    removeFeature, toggleGroup, setFeature, setStatus, setQuery,
  }
}
