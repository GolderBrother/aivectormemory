<script lang="ts" setup>
import { ref, watch } from 'vue'

const props = defineProps<{ placeholder?: string; modelValue?: string }>()
const emit = defineEmits<{ 'update:modelValue': [value: string]; search: [value: string] }>()

const input = ref(props.modelValue || '')
let timer: ReturnType<typeof setTimeout>

watch(() => props.modelValue, v => { input.value = v || '' })

function onInput() {
  emit('update:modelValue', input.value)
  clearTimeout(timer)
  timer = setTimeout(() => emit('search', input.value), 300)
}

function clear() {
  input.value = ''
  emit('update:modelValue', '')
  emit('search', '')
}
</script>

<template>
  <div class="search-box">
    <svg class="search-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round">
      <circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/>
    </svg>
    <input
      class="search-input"
      :placeholder="placeholder"
      v-model="input"
      @input="onInput"
    />
    <button v-if="input" class="search-clear" @click="clear">×</button>
  </div>
</template>

<style scoped>
.search-box { position: relative; flex: 1; max-width: 360px; }
.search-icon {
  position: absolute; left: 12px; top: 50%; transform: translateY(-50%);
  width: 16px; height: 16px; color: var(--text-muted);
}
.search-input {
  width: 100%; padding: 9px 32px 9px 36px;
  border: 1px solid var(--border); border-radius: var(--radius);
  font-size: var(--font-size-base); color: var(--text-primary);
  background: var(--bg-surface); font-family: inherit;
  transition: all var(--transition-fast);
}
.search-input:focus { outline: none; border-color: var(--accent); box-shadow: var(--shadow-focus); }
.search-input::placeholder { color: var(--text-dim); }
.search-clear {
  position: absolute; right: 8px; top: 50%; transform: translateY(-50%);
  width: 20px; height: 20px; border: none; background: transparent;
  color: var(--text-muted); font-size: 16px; cursor: pointer;
  display: flex; align-items: center; justify-content: center;
  border-radius: 50%; padding: 0; line-height: 1;
}
.search-clear:hover { color: var(--text-primary); background: var(--border); }
</style>
