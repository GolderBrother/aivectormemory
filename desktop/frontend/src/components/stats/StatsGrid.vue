<script lang="ts" setup>
import { useI18n } from 'vue-i18n'

const { t } = useI18n()

const props = defineProps<{
  memories?: { project?: number; user?: number }
  issues?: { pending?: number; in_progress?: number; completed?: number; archived?: number }
}>()

const emit = defineEmits<{
  (e: 'card-click', type: 'memories' | 'issues', scope?: string, status?: string): void
}>()

interface Card {
  label: string
  num: number
  cls: string
  sub?: string
  type: 'memories' | 'issues'
  scope?: string
  status?: string
}

function getCards(): Card[] {
  const mem = props.memories || {}
  const iss = props.issues || {}
  return [
    { label: t('projectMemories'), num: mem.project || 0, cls: 'blue', type: 'memories', scope: 'project' },
    { label: t('globalMemories'), num: mem.user || 0, cls: 'purple', type: 'memories', scope: 'user' },
    { label: t('status.pending'), num: iss.pending || 0, cls: 'orange', sub: `${iss.in_progress || 0} ${t('status.in_progress')}`, type: 'issues', status: 'pending' },
    { label: t('status.completed'), num: iss.completed || 0, cls: 'green', sub: `${iss.archived || 0} ${t('status.archived')}`, type: 'issues', status: 'completed' },
  ]
}

function onCardClick(card: Card) {
  emit('card-click', card.type, card.scope, card.status)
}
</script>

<template>
  <div class="stats-grid">
    <div
      v-for="card in getCards()"
      :key="card.label"
      :class="['stat-card', 'glass-card']"
      @click="onCardClick(card)"
    >
      <div class="stat-label">{{ card.label }}</div>
      <div :class="['stat-value', card.cls]">{{ card.num }}</div>
      <div v-if="card.sub" class="stat-sub">{{ card.sub }}</div>
    </div>
  </div>
</template>

<style scoped>
.stats-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 20px; }
.stat-card { padding: 16px; cursor: pointer; transition: all 0.2s ease; }
.stat-card:hover { border-color: var(--border-hover); transform: translateY(-1px); }
.stat-label {
  font-size: 11px;
  color: var(--text-muted);
  text-transform: uppercase;
  letter-spacing: 0.05em;
  margin-bottom: 8px;
}
.stat-value { font-size: 26px; font-weight: 700; line-height: 1.2; }
.stat-value.blue { color: var(--accent); }
.stat-value.green { color: var(--color-success); }
.stat-value.purple { color: var(--color-purple); }
.stat-value.orange { color: var(--color-warning); }
.stat-sub { font-size: 11px; color: var(--text-muted); margin-top: 4px; }

@media (max-width: 900px) {
  .stats-grid { grid-template-columns: repeat(2, 1fr); }
}
</style>
