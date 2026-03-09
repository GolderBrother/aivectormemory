import { defineStore } from 'pinia'
import { ref, watch } from 'vue'

export type ThemeMode = 'dark' | 'light' | 'system'

export const useThemeStore = defineStore('theme', () => {
  const mode = ref<ThemeMode>('dark')
  const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)')

  function resolvedTheme(): 'dark' | 'light' {
    return mode.value === 'system'
      ? (mediaQuery.matches ? 'dark' : 'light')
      : mode.value
  }

  function apply() {
    const theme = resolvedTheme()
    document.documentElement.setAttribute('data-theme', theme)
  }

  function setMode(m: ThemeMode) {
    mode.value = m
    apply()
  }

  // Listen for system theme changes
  mediaQuery.addEventListener('change', () => {
    if (mode.value === 'system') apply()
  })

  // Initialize
  watch(mode, apply, { immediate: true })

  return { mode, setMode, resolvedTheme }
})
