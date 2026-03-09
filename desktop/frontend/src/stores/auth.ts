import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import { Register, Login, Logout, GetCurrentUser } from '../../wailsjs/go/main/App'

const TOKEN_KEY = 'avm-token'

export const useAuthStore = defineStore('auth', () => {
  const username = ref('')
  const token = ref(localStorage.getItem(TOKEN_KEY) || '')
  const isLoggedIn = computed(() => !!username.value && !!token.value)

  async function register(user: string, password: string) {
    await Register(user, password)
    return login(user, password)
  }

  async function login(user: string, password: string) {
    const result = await Login(user, password)
    token.value = result.token
    username.value = result.username
    localStorage.setItem(TOKEN_KEY, result.token)
  }

  async function logout() {
    if (token.value) {
      await Logout(token.value).catch(() => {})
    }
    token.value = ''
    username.value = ''
    localStorage.removeItem(TOKEN_KEY)
  }

  async function restore(): Promise<boolean> {
    if (!token.value) return false
    try {
      const result = await GetCurrentUser(token.value)
      username.value = result.username
      return true
    } catch {
      token.value = ''
      localStorage.removeItem(TOKEN_KEY)
      return false
    }
  }

  return { username, token, isLoggedIn, register, login, logout, restore }
})
