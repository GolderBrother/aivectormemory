import { defineStore } from 'pinia'
import { ref } from 'vue'
import { GetProjects, AddProject, DeleteProject, GetStats } from '../../wailsjs/go/main/App'

export interface Project {
  project_dir: string
  name: string
  memories: number
  user_memories: number
  issues: number
  tags: number
}

export const useProjectStore = defineStore('project', () => {
  const projects = ref<Project[]>([])
  const current = ref<string>('')
  const stats = ref<Record<string, any>>({})

  async function loadProjects() {
    projects.value = await GetProjects() || []
  }

  async function addProject(dir: string) {
    await AddProject(dir)
    await loadProjects()
  }

  async function deleteProject(dir: string) {
    await DeleteProject(dir)
    await loadProjects()
    if (current.value === dir) current.value = ''
  }

  async function loadStats() {
    if (!current.value) return
    stats.value = await GetStats(current.value) || {}
  }

  function setCurrent(dir: string) {
    current.value = dir
  }

  return { projects, current, stats, loadProjects, addProject, deleteProject, loadStats, setCurrent }
})
