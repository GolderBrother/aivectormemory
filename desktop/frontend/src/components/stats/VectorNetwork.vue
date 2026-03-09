<script lang="ts" setup>
import { ref, onMounted, onUnmounted, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import Modal from '../layout/Modal.vue'

const { t } = useI18n()

const props = defineProps<{
  tags: [string, number][]
}>()

const emit = defineEmits<{
  (e: 'tag-click', tag: string): void
}>()

const svgRef = ref<SVGSVGElement | null>(null)
const showAllModal = ref(false)

const MAX_NODES = 100
const FL = 600
const W = 800
const H = 600
const COLORS = ['#3B82F6','#2563EB','#60A5FA','#818CF8','#A78BFA','#34D399','#F59E0B','#EF4444','#EC4899','#14B8A6','#8B5CF6','#F97316','#06B6D4','#84CC16','#E879F9']

interface Node3D {
  label: string; count: number; baseR: number
  x: number; y: number; z: number
  px: number; py: number; pz: number
}

let nodes3d: Node3D[] = []
let edges: [number, number][] = []
let rotY = 0
let rotX = 0.3
const autoSpeed = 0.003
let isHovering = false
let dragging = false
let lastX = 0, lastY = 0, startX = 0, startY = 0, didDrag = false
let animId = 0

function buildGraph() {
  const items = props.tags.slice(0, MAX_NODES)
  if (!items.length) { nodes3d = []; edges = []; return }

  const nodeCount = items.length
  const R = Math.min(200, 80 + nodeCount * 2)
  const maxC = items[0][1], minC = items[items.length - 1][1] || 1

  nodes3d = items.map(([label, count], i) => {
    const ratio = maxC === minC ? 0.5 : (count - minC) / (maxC - minC)
    const baseR = 4 + ratio * (nodeCount > 30 ? 8 : 14)
    const phi = Math.acos(1 - 2 * (i + 0.5) / nodeCount)
    const theta = Math.PI * (1 + Math.sqrt(5)) * i
    return {
      label, count, baseR,
      x: R * Math.sin(phi) * Math.cos(theta),
      y: R * Math.cos(phi),
      z: R * Math.sin(phi) * Math.sin(theta),
      px: 0, py: 0, pz: 0,
    }
  })

  edges = []
  for (let i = 0; i < nodes3d.length; i++) {
    if (i + 1 < nodes3d.length) edges.push([i, i + 1])
    if (i + 3 < nodes3d.length) edges.push([i, i + 3])
  }
  if (nodes3d.length > 2) edges.push([nodes3d.length - 1, 0])
}

function render() {
  const svg = svgRef.value
  if (!svg || !nodes3d.length) return

  const ns = 'http://www.w3.org/2000/svg'
  svg.innerHTML = ''
  const R = Math.min(200, 80 + nodes3d.length * 2)

  const cosY = Math.cos(rotY), sinY = Math.sin(rotY)
  const cosX = Math.cos(rotX), sinX = Math.sin(rotX)

  nodes3d.forEach(n => {
    const x1 = n.x * cosY - n.z * sinY
    const z1 = n.x * sinY + n.z * cosY
    n.px = x1; n.py = n.y * cosX - z1 * sinX; n.pz = n.y * sinX + z1 * cosX
  })

  // Draw edges
  edges.forEach(([a, b]) => {
    const na = nodes3d[a], nb = nodes3d[b]
    const sa = FL / (FL + na.pz), sb = FL / (FL + nb.pz)
    const line = document.createElementNS(ns, 'line')
    line.setAttribute('x1', (W / 2 + na.px * sa).toFixed(1))
    line.setAttribute('y1', (H / 2 + na.py * sa).toFixed(1))
    line.setAttribute('x2', (W / 2 + nb.px * sb).toFixed(1))
    line.setAttribute('y2', (H / 2 + nb.py * sb).toFixed(1))
    line.setAttribute('class', a % 3 === 0 ? 'vg-edge vg-edge--weak' : 'vg-edge')
    line.style.opacity = (0.08 + 0.25 * ((na.pz + nb.pz) / 2 + R) / (2 * R)).toFixed(2)
    svg.appendChild(line)
  })

  // Draw nodes sorted by depth
  const order = nodes3d.map((_, i) => i).sort((a, b) => nodes3d[b].pz - nodes3d[a].pz)
  order.forEach(i => {
    const n = nodes3d[i]
    const s = FL / (FL + n.pz)
    const depth = (n.pz + R) / (2 * R)
    const cr = n.baseR * s, gr = cr * 2.5
    const color = COLORS[i % COLORS.length]

    const g = document.createElementNS(ns, 'g')
    g.setAttribute('class', 'vg-node')
    g.setAttribute('transform', `translate(${(W / 2 + n.px * s).toFixed(1)},${(H / 2 + n.py * s).toFixed(1)})`)
    g.style.opacity = (0.4 + 0.6 * depth).toFixed(2)
    g.dataset.idx = String(i)

    const glow = document.createElementNS(ns, 'circle')
    glow.setAttribute('r', gr.toFixed(1))
    glow.setAttribute('class', 'vg-node__glow')
    glow.style.fill = color + '15'

    const core = document.createElementNS(ns, 'circle')
    core.setAttribute('r', cr.toFixed(1))
    core.setAttribute('class', 'vg-node__core')
    core.style.fill = color

    const text = document.createElementNS(ns, 'text')
    text.setAttribute('class', 'vg-node__label')
    text.setAttribute('dy', (gr + 10).toFixed(0))
    text.textContent = n.label

    g.appendChild(glow); g.appendChild(core); g.appendChild(text)
    svg.appendChild(g)
  })
}

function animate() {
  if (!isHovering) rotY += autoSpeed
  render()
  animId = requestAnimationFrame(animate)
}

function onMouseDown(e: MouseEvent) {
  dragging = true; didDrag = false
  startX = e.clientX; startY = e.clientY
  lastX = e.clientX; lastY = e.clientY
  e.preventDefault()
}

function onMouseMove(e: MouseEvent) {
  if (!dragging) return
  const dx = e.clientX - startX, dy = e.clientY - startY
  if (dx * dx + dy * dy > 9) didDrag = true
  rotY += (e.clientX - lastX) * 0.008
  rotX = Math.max(-1.2, Math.min(1.2, rotX + (e.clientY - lastY) * 0.008))
  lastX = e.clientX; lastY = e.clientY
}

function onMouseUp(e: MouseEvent) {
  dragging = false
  if (!didDrag) {
    const el = document.elementFromPoint(e.clientX, e.clientY)
    const g = el?.closest('.vg-node') as SVGGElement | null
    if (g?.dataset.idx) {
      const idx = parseInt(g.dataset.idx)
      if (nodes3d[idx]) emit('tag-click', nodes3d[idx].label)
    }
  }
}

onMounted(() => {
  buildGraph()
  if (nodes3d.length) animate()
})

onUnmounted(() => {
  if (animId) cancelAnimationFrame(animId)
})

watch(() => props.tags, () => {
  buildGraph()
  if (!animId && nodes3d.length) animate()
})

function onTagLinkClick(tag: string) {
  showAllModal.value = false
  emit('tag-click', tag)
}
</script>

<template>
  <div v-if="tags.length" class="vector-network">
    <div class="vector-network__header">
      <div class="vector-network__left">
        <span class="vector-network__label">{{ t('vectorNetwork') }}</span>
        <span class="vector-network__sub">{{ t('vectorSub', { n: Math.min(tags.length, MAX_NODES) }) }}</span>
      </div>
      <a v-if="tags.length" class="vector-network__more" @click="showAllModal = true">{{ t('showMore') }}</a>
    </div>
    <svg
      ref="svgRef"
      class="vector-graph"
      :viewBox="`0 0 ${W} ${H}`"
      preserveAspectRatio="xMidYMid meet"
      @mouseenter="isHovering = true"
      @mouseleave="isHovering = false; dragging = false"
      @mousedown="onMouseDown"
      @mousemove="onMouseMove"
      @mouseup="onMouseUp"
    />
  </div>

  <!-- All tags modal -->
  <Modal :show="showAllModal" :title="t('allTags', { n: tags.length })" hide-footer @close="showAllModal = false" width="540px">
    <ul class="stat-list stat-list--tags">
      <li v-for="[tag, count] in tags" :key="tag">
        <a class="stat-link" @click="onTagLinkClick(tag)">
          <span>{{ tag }}</span>
          <span class="tag-count">{{ count }}</span>
        </a>
      </li>
    </ul>
  </Modal>
</template>

<style scoped>
.vector-network {
  margin-top: 20px; background: var(--glass-bg); border-radius: var(--radius-lg);
  border: 1px solid var(--glass-border); box-shadow: var(--glass-shadow); padding: 20px 24px;
  display: flex; flex-direction: column; flex: 1; min-height: 0;
}
.vector-network__header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px; }
.vector-network__left { display: flex; align-items: baseline; gap: 12px; }
.vector-network__label {
  font-family: var(--font-mono); font-size: 12px; font-weight: 600;
  text-transform: uppercase; letter-spacing: 0.8px; color: var(--text-muted);
}
.vector-network__sub { font-size: 12px; color: var(--text-dim); }
.vector-network__more { font-size: 12px; color: var(--accent-light); cursor: pointer; text-decoration: none; white-space: nowrap; }
.vector-network__more:hover { color: var(--accent); }

.vector-graph { width: 100%; flex: 1; min-height: 300px; user-select: none; cursor: grab; }
.vector-graph:active { cursor: grabbing; }

:deep(.vg-edge) { stroke: var(--border); stroke-width: 1; transition: stroke 0.2s; }
:deep(.vg-edge--weak) { stroke: var(--bg-surface); stroke-width: 0.5; stroke-dasharray: 3 3; }
:deep(.vg-node) { cursor: pointer; transition: opacity 0.15s; }
:deep(.vg-node__glow) { transition: fill 0.2s; }
:deep(.vg-node__core) { transition: filter 0.2s; }
:deep(.vg-node__label) {
  font-family: var(--font-mono); font-size: 11px; fill: var(--text-secondary);
  text-anchor: middle; pointer-events: none; transition: fill 0.15s;
}

/* All tags modal list */
.stat-list { list-style: none; padding: 0; }
.stat-list li {
  display: flex; justify-content: space-between; align-items: center;
  padding: 8px 0; font-size: 13px; color: var(--text-heading);
  border-bottom: 1px solid var(--border-light);
}
.stat-list li:last-child { border-bottom: none; }
.stat-link {
  display: flex; justify-content: space-between; align-items: center; width: 100%;
  color: inherit; text-decoration: none; cursor: pointer; padding: 2px 0;
  border-radius: 4px; transition: color var(--transition-fast);
}
.stat-link:hover { color: var(--accent); }
.tag-count { font-family: var(--font-mono); font-weight: 600; color: var(--accent); font-size: 13px; }
</style>
