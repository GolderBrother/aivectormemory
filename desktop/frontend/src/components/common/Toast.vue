<script lang="ts" setup>
import { ref } from 'vue'

export interface ToastMessage {
  id: number
  text: string
  type: 'success' | 'error' | 'warning' | 'info'
}

const messages = ref<ToastMessage[]>([])
let nextId = 0

function show(text: string, type: ToastMessage['type'] = 'success') {
  const id = nextId++
  messages.value.push({ id, text, type })
  setTimeout(() => {
    messages.value = messages.value.filter(m => m.id !== id)
  }, 3000)
}

defineExpose({ show })

// Global toast access
if (typeof window !== 'undefined') {
  ;(window as any).__toast = { show }
}
</script>

<template>
  <div class="toast-container">
    <div
      v-for="msg in messages"
      :key="msg.id"
      class="toast"
      :class="`toast--${msg.type}`"
    >
      {{ msg.text }}
    </div>
  </div>
</template>

<style scoped>
.toast-container {
  position: fixed; top: 20px; right: 20px; z-index: 200;
  display: flex; flex-direction: column; gap: 8px; pointer-events: none;
}
.toast {
  padding: 12px 20px; border-radius: var(--radius-md); font-size: var(--font-size-base);
  color: var(--text-primary); pointer-events: auto; max-width: 400px;
  backdrop-filter: blur(12px); border: 1px solid rgba(255,255,255,0.06);
  animation: toastIn 0.3s ease, toastOut 0.3s ease 2.7s forwards;
}
.toast--success { background: var(--color-success-bg); border-color: rgba(34,197,94,0.3); }
.toast--info { background: var(--color-info-bg); border-color: rgba(59,130,246,0.3); }
.toast--warning { background: var(--color-warning-bg); border-color: rgba(245,158,11,0.3); }
.toast--error { background: var(--color-error-bg); border-color: rgba(239,68,68,0.3); }
</style>
