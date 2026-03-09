import { ref } from 'vue'
import { GetTags, RenameTag, MergeTags, DeleteTags, GetMemories } from '../../wailsjs/go/main/App'
import { useProjectStore } from '../stores/project'

export function useTags() {
  const projectStore = useProjectStore()
  const tags = ref<any[]>([])
  const loading = ref(false)
  const query = ref('')
  const selectedTags = ref<Set<string>>(new Set())

  async function load() {
    loading.value = true
    try {
      const data = await GetTags(projectStore.current, query.value)
      tags.value = data || []
    } catch {
      tags.value = []
    } finally {
      loading.value = false
    }
  }

  async function rename(oldName: string, newName: string) {
    return await RenameTag(projectStore.current, oldName, newName)
  }

  async function merge(sources: string[], target: string) {
    return await MergeTags(projectStore.current, sources, target)
  }

  async function remove(names: string[]) {
    return await DeleteTags(projectStore.current, names)
  }

  async function getMemoriesByTag(tag: string, topK = 50) {
    const result = await GetMemories('all', projectStore.current, '', tag, '', topK, 0)
    return result?.memories || []
  }

  function toggleSelect(name: string) {
    const s = new Set(selectedTags.value)
    s.has(name) ? s.delete(name) : s.add(name)
    selectedTags.value = s
  }

  function selectAll() {
    if (selectedTags.value.size === tags.value.length) {
      selectedTags.value = new Set()
    } else {
      selectedTags.value = new Set(tags.value.map((t: any) => t.name || t.tag))
    }
  }

  function clearSelection() { selectedTags.value = new Set() }

  function setQuery(q: string) { query.value = q; load() }

  return {
    tags, loading, query, selectedTags,
    load, rename, merge, remove, getMemoriesByTag,
    toggleSelect, selectAll, clearSelection, setQuery,
  }
}
