<script lang="ts" setup>
import { onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { useSettingsStore } from './stores/settings'
import { useThemeStore } from './stores/theme'
import { useProjectStore } from './stores/project'
import { useAuthStore } from './stores/auth'
import { useI18n } from 'vue-i18n'
import Toast from './components/common/Toast.vue'

const router = useRouter()
const settingsStore = useSettingsStore()
const themeStore = useThemeStore()
const projectStore = useProjectStore()
const authStore = useAuthStore()
const { locale } = useI18n()

onMounted(async () => {
  await settingsStore.load()
  const s = settingsStore.settings
  themeStore.setMode((s.theme || 'dark') as any)
  locale.value = s.language || 'zh-CN'

  const restored = await authStore.restore()
  if (!restored) return

  if (s.last_project) {
    await projectStore.loadProjects()
    const exists = projectStore.projects.some(p => p.project_dir === s.last_project)
    if (exists) {
      projectStore.setCurrent(s.last_project)
      router.replace('/project/stats')
    }
  }
})
</script>

<template>
  <router-view />
  <Toast />
</template>

<style>
@import './styles/base.css';
</style>
