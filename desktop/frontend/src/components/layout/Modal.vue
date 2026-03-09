<script lang="ts" setup>
import { useI18n } from 'vue-i18n'

const { t } = useI18n()

const props = defineProps<{
  show: boolean
  title?: string
  danger?: boolean
  confirmText?: string
  hideFooter?: boolean
  width?: string
}>()

const emit = defineEmits<{
  close: []
  confirm: []
}>()

function onOverlay() {
  emit('close')
}
</script>

<template>
  <Teleport to="body">
    <div v-if="show" class="modal">
      <div class="modal-overlay" @click="onOverlay" />
      <div class="modal-body" :style="width ? { width } : {}">
        <div class="modal-header">
          <h3>{{ title }}</h3>
          <button class="modal-close" @click="emit('close')">×</button>
        </div>
        <div class="modal-content">
          <slot />
        </div>
        <div v-if="!hideFooter" class="modal-footer">
          <button class="btn btn--secondary" @click="emit('close')">{{ t('cancel') }}</button>
          <button
            class="btn"
            :class="danger ? 'btn--danger' : 'btn--primary'"
            @click="emit('confirm')"
          >
            {{ confirmText || t('save') }}
          </button>
        </div>
      </div>
    </div>
  </Teleport>
</template>

<style scoped>
.modal {
  position: fixed; top: 0; left: 0; width: 100%; height: 100%;
  z-index: 100; display: flex; align-items: center; justify-content: center;
}
.modal-overlay {
  position: absolute; top: 0; left: 0; width: 100%; height: 100%;
  background: var(--overlay); backdrop-filter: blur(4px);
}
.modal-body {
  position: relative; background: var(--bg-surface); border-radius: var(--radius-xl);
  padding: 28px; width: 80%; max-width: 640px; max-height: 80vh; overflow-y: auto;
  border: 1px solid var(--border); box-shadow: var(--shadow-modal);
}
.modal-header {
  display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px;
}
.modal-header h3 {
  font-family: var(--font-mono); font-size: var(--font-size-lg); font-weight: 600; color: var(--text-primary);
}
.modal-close {
  background: none; border: none; font-size: 22px; cursor: pointer;
  color: var(--text-muted); width: 32px; height: 32px;
  display: flex; align-items: center; justify-content: center;
  border-radius: var(--radius); transition: all var(--transition-fast);
}
.modal-close:hover { background: var(--border); color: var(--text-primary); }
.modal-footer {
  display: flex; justify-content: flex-end; gap: 8px; margin-top: 24px;
  padding-top: 16px; border-top: 1px solid var(--border);
}
</style>
