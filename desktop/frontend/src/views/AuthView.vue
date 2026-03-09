<script lang="ts" setup>
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { useI18n } from 'vue-i18n'
import { useAuthStore } from '../stores/auth'

const router = useRouter()
const { t } = useI18n()
const authStore = useAuthStore()

const mode = ref<'login' | 'register'>('login')
const username = ref('')
const password = ref('')
const confirmPassword = ref('')
const loading = ref(false)
const error = ref('')

function switchMode(m: 'login' | 'register') {
  mode.value = m
  error.value = ''
  username.value = ''
  password.value = ''
  confirmPassword.value = ''
}

async function handleSubmit() {
  error.value = ''
  const user = username.value.trim()
  const pass = password.value

  if (!user || !pass) {
    error.value = t('auth.fieldsRequired')
    return
  }

  if (mode.value === 'register') {
    if (pass.length < 6) {
      error.value = t('auth.passwordTooShort')
      return
    }
    if (pass !== confirmPassword.value) {
      error.value = t('auth.passwordMismatch')
      return
    }
  }

  loading.value = true
  try {
    if (mode.value === 'register') {
      await authStore.register(user, pass)
    } else {
      await authStore.login(user, pass)
    }
    router.replace('/')
  } catch (e: any) {
    error.value = e?.message || 'Failed'
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <div class="auth-page">
    <div class="auth-card">
      <!-- Logo -->
      <div class="auth-logo">
        <svg viewBox="0 0 40 40" width="36" height="36">
          <line x1="20" y1="8" x2="10" y2="28" stroke="currentColor" stroke-width="1.2" opacity="0.5" />
          <line x1="20" y1="8" x2="30" y2="28" stroke="currentColor" stroke-width="1.2" opacity="0.5" />
          <line x1="10" y1="28" x2="30" y2="28" stroke="currentColor" stroke-width="1.2" opacity="0.5" />
          <circle cx="20" cy="8" r="4" fill="var(--accent)" />
          <circle cx="10" cy="28" r="4" fill="var(--accent)" />
          <circle cx="30" cy="28" r="4" fill="var(--accent)" />
        </svg>
        <span>AIVectorMemory</span>
      </div>

      <!-- Title -->
      <div class="auth-title">
        {{ mode === 'login' ? t('auth.login') : t('auth.createAccount') }}
      </div>
      <div class="auth-desc">
        {{ mode === 'login' ? t('auth.loginDesc') : t('auth.createAccountDesc') }}
      </div>

      <!-- Error -->
      <div v-if="error" class="auth-error">{{ error }}</div>

      <!-- Form -->
      <form @submit.prevent="handleSubmit">
        <div class="form-group">
          <label class="form-label">{{ t('auth.username') }}</label>
          <input v-model="username" class="form-input" type="text" :placeholder="t('auth.usernamePlaceholder')" autocomplete="username" />
        </div>

        <div class="form-group">
          <label class="form-label">{{ t('auth.password') }}</label>
          <input v-model="password" class="form-input" type="password" :placeholder="t('auth.passwordPlaceholder')" autocomplete="current-password" />
        </div>

        <div v-if="mode === 'register'" class="form-group">
          <label class="form-label">{{ t('auth.confirmPassword') }}</label>
          <input v-model="confirmPassword" class="form-input" type="password" :placeholder="t('auth.confirmPasswordPlaceholder')" autocomplete="new-password" />
        </div>

        <button class="btn btn-primary btn-full" type="submit" :disabled="loading">
          {{ loading ? '...' : (mode === 'login' ? t('auth.login') : t('auth.createAccount')) }}
        </button>
      </form>

      <!-- Switch mode -->
      <div class="auth-switch">
        <template v-if="mode === 'login'">
          {{ t('auth.noAccount') }}
          <button class="btn-link" @click="switchMode('register')">{{ t('auth.register') }}</button>
        </template>
        <template v-else>
          {{ t('auth.hasAccount') }}
          <button class="btn-link" @click="switchMode('login')">{{ t('auth.login') }}</button>
        </template>
      </div>
    </div>
  </div>
</template>

<style scoped>
.auth-page {
  width: 100%; min-height: 100vh; display: flex; align-items: center; justify-content: center;
  position: relative; overflow: hidden;
}
.auth-page::before {
  content: ''; position: fixed; top: -30%; left: -10%;
  width: 600px; height: 600px;
  background: radial-gradient(circle, rgba(59,130,246,0.08) 0%, transparent 70%);
  animation: floatOrb1 20s ease-in-out infinite; pointer-events: none;
}
.auth-page::after {
  content: ''; position: fixed; bottom: -20%; right: -10%;
  width: 500px; height: 500px;
  background: radial-gradient(circle, rgba(139,92,246,0.06) 0%, transparent 70%);
  animation: floatOrb2 25s ease-in-out infinite; pointer-events: none;
}
@keyframes floatOrb1 {
  0%,100% { transform: translate(0,0); }
  33% { transform: translate(80px,60px); }
  66% { transform: translate(-40px,30px); }
}
@keyframes floatOrb2 {
  0%,100% { transform: translate(0,0); }
  33% { transform: translate(-60px,-40px); }
  66% { transform: translate(40px,-60px); }
}

.auth-card {
  width: 420px; max-width: 90vw; position: relative; z-index: 1;
  background: rgba(30,41,59,0.6); backdrop-filter: blur(20px); -webkit-backdrop-filter: blur(20px);
  border-radius: 16px; padding: 40px 32px; border: 1px solid var(--border-light);
}

.auth-logo {
  display: flex; align-items: center; justify-content: center; gap: 10px; margin-bottom: 28px;
  color: var(--accent);
}
.auth-logo span {
  font-family: var(--font-mono); font-size: 18px; font-weight: 700; letter-spacing: -0.3px;
  color: var(--text-primary);
}

.auth-title { text-align: center; font-size: 18px; font-weight: 600; color: var(--text-primary); margin-bottom: 4px; }
.auth-desc { text-align: center; font-size: 13px; color: var(--text-muted); margin-bottom: 24px; }

.auth-error {
  background: var(--color-error-bg); color: var(--color-error-light); border: 1px solid rgba(239,68,68,0.3);
  border-radius: var(--radius); padding: 10px 14px; font-size: 13px; margin-bottom: 16px; text-align: center;
}

.form-group { margin-bottom: 16px; }
.form-label { display: block; font-size: 13px; font-weight: 500; color: var(--text-muted); margin-bottom: 6px; }
.form-input {
  width: 100%; padding: 10px 12px; background: var(--bg-input); border: 1px solid var(--border-light);
  border-radius: var(--radius); color: var(--text-primary); font-size: 14px; outline: none;
  transition: border-color 0.15s;
}
.form-input:focus { border-color: var(--accent); box-shadow: 0 0 0 2px rgba(59,130,246,0.12); }
.form-input::placeholder { color: var(--text-dim); }

.btn { padding: 10px 16px; border: none; border-radius: var(--radius); cursor: pointer; font-size: 14px; font-weight: 500; transition: all 0.15s; }
.btn-primary { background: var(--accent); color: #fff; }
.btn-primary:hover { filter: brightness(1.1); }
.btn-primary:disabled { opacity: 0.6; cursor: not-allowed; }
.btn-full { width: 100%; display: flex; align-items: center; justify-content: center; margin-top: 8px; }

.btn-link { background: none; border: none; color: var(--accent); cursor: pointer; font-size: 13px; padding: 0; }
.btn-link:hover { text-decoration: underline; }

.auth-switch { text-align: center; font-size: 13px; color: var(--text-muted); margin-top: 20px; }
</style>
