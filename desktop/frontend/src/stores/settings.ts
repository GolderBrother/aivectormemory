import { defineStore } from 'pinia'
import { ref } from 'vue'
import { GetSettings, SaveSettings } from '../../wailsjs/go/main/App'

export interface Settings {
  theme: string
  language: string
  db_path: string
  python_path: string
  web_port: number
  auto_start: boolean
  last_project: string
  window_width: number
  window_height: number
  window_x: number
  window_y: number
}

export const useSettingsStore = defineStore('settings', () => {
  const settings = ref<Settings>({
    theme: 'dark',
    language: 'zh-CN',
    db_path: '',
    python_path: '',
    web_port: 9080,
    auto_start: false,
    last_project: '',
    window_width: 1200,
    window_height: 800,
    window_x: -1,
    window_y: -1,
  })

  async function load() {
    const s = await GetSettings()
    if (s) settings.value = s as Settings
  }

  async function save(partial?: Partial<Settings>) {
    if (partial) Object.assign(settings.value, partial)
    await SaveSettings(settings.value as any)
  }

  return { settings, load, save }
})
