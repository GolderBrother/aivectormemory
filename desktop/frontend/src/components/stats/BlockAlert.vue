<script lang="ts" setup>
import { useI18n } from 'vue-i18n'
import { useRouter } from 'vue-router'

const { t } = useI18n()
const router = useRouter()

defineProps<{
  isBlocked: boolean
  blockReason?: string
  currentTask?: string
}>()

function goToStatus() { router.push('/project/status') }
</script>

<template>
  <div :class="['alert', isBlocked ? 'alert-danger' : 'alert-ok']" @click="goToStatus" style="cursor: pointer">
    <div class="alert-dot" />
    <div class="alert-body">
      <span class="alert-title">{{ isBlocked ? t('blocked') : t('normalStatus') }}</span>
      <span v-if="isBlocked && blockReason" class="alert-reason">{{ blockReason }}</span>
      <span v-if="currentTask" class="alert-task">{{ t('currentTask') }}: {{ currentTask }}</span>
    </div>
  </div>
</template>

<style scoped>
.alert {
  padding: 10px 14px;
  border-radius: var(--radius-sm);
  margin-bottom: 16px;
  display: flex;
  align-items: flex-start;
  gap: 10px;
  font-size: 13px;
  transition: all 0.2s;
}
.alert:hover { box-shadow: var(--shadow-card); }
.alert-danger {
  background: hsl(0 84% 60% / 0.08);
  border: 1px solid hsl(0 84% 60% / 0.25);
  color: hsl(0 86% 80%);
}
.alert-ok {
  background: hsl(142 71% 45% / 0.08);
  border: 1px solid hsl(142 71% 45% / 0.25);
  color: hsl(142 71% 75%);
}
.alert-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  flex-shrink: 0;
  margin-top: 5px;
  animation: pulse 2s infinite;
}
.alert-danger .alert-dot { background: var(--color-error); }
.alert-ok .alert-dot { background: var(--color-success); animation: none; }
.alert-body { display: flex; flex-direction: column; gap: 4px; }
.alert-title { font-weight: 600; }
.alert-reason { color: var(--text-primary); }
.alert-task { font-size: 12px; color: var(--text-secondary); }
</style>
