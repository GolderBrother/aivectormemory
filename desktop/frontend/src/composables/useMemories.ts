import { ref } from 'vue'
import {
  GetMemories, GetMemoryDetail, UpdateMemory, DeleteMemory,
  DeleteMemoriesBatch, SearchMemories, ExportMemories, ImportMemories,
} from '../../wailsjs/go/main/App'
import { useProjectStore } from '../stores/project'

const PAGE_SIZE = 20

export function useMemories(defaultScope: string) {
  const projectStore = useProjectStore()
  const memories = ref<any[]>([])
  const total = ref(0)
  const page = ref(1)
  const query = ref('')
  const loading = ref(false)

  async function load() {
    loading.value = true
    try {
      const dir = projectStore.current
      const offset = (page.value - 1) * PAGE_SIZE
      const data = await GetMemories(defaultScope, dir, query.value, '', '', PAGE_SIZE, offset)
      memories.value = data?.memories || []
      total.value = data?.total || 0
    } catch {
      memories.value = []
      total.value = 0
    } finally {
      loading.value = false
    }
  }

  async function getDetail(id: string) {
    return await GetMemoryDetail(id)
  }

  async function update(id: string, content: string, tags: string[], scope: string) {
    await UpdateMemory(id, content, tags, scope)
  }

  async function remove(id: string) {
    await DeleteMemory(id)
  }

  async function batchDelete(ids: string[]) {
    return await DeleteMemoriesBatch(ids)
  }

  async function search(q: string, tags: string[] = [], topK = 50) {
    return await SearchMemories(projectStore.current, q, 'all', tags, topK)
  }

  async function exportData(scope: string) {
    return await ExportMemories(projectStore.current, scope)
  }

  async function importData(jsonStr: string) {
    return await ImportMemories(projectStore.current, jsonStr)
  }

  function setPage(p: number) {
    page.value = p
    load()
  }

  function setQuery(q: string) {
    query.value = q
    page.value = 1
    load()
  }

  return {
    memories, total, page, query, loading,
    load, getDetail, update, remove, batchDelete,
    search, exportData, importData, setPage, setQuery,
    PAGE_SIZE,
  }
}
