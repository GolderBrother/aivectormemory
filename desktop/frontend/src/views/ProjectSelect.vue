<script lang="ts" setup>
import { ref, onMounted, computed } from 'vue'
import { useRouter } from 'vue-router'
import { useI18n } from 'vue-i18n'
import { useProjectStore, type Project } from '../stores/project'
import { useSettingsStore } from '../stores/settings'
import { LANGS } from '../i18n'
import Modal from '../components/layout/Modal.vue'
import { BrowseDirectory, SelectDirectory, SetLanguage, CheckEnvironment, CheckUpgrade, InstallPackage } from '../../wailsjs/go/main/App'

const router = useRouter()
const { t, locale } = useI18n()
const projectStore = useProjectStore()
const settingsStore = useSettingsStore()

const showAddModal = ref(false)
const showDeleteModal = ref(false)
const deleteTarget = ref<Project | null>(null)
const addPath = ref('')
const browsing = ref(false)
const browseCurrentPath = ref('')
const browseDirsList = ref<string[]>([])

const projects = computed(() => projectStore.projects)

// Environment detection
const envStatus = ref<any>(null)
const upgradeInfo = ref<any>(null)
const installing = ref(false)
const installResult = ref('') // 'success' | 'error'

onMounted(async () => {
  projectStore.loadProjects()
  envStatus.value = await CheckEnvironment()
  if (envStatus.value?.avm_installed) {
    CheckUpgrade(envStatus.value.avm_version).then(info => { upgradeInfo.value = info })
  }
})

async function handleInstall(upgrade = false) {
  installing.value = true
  installResult.value = ''
  try {
    await InstallPackage(upgrade)
    installResult.value = 'success'
    envStatus.value = await CheckEnvironment()
    if (envStatus.value?.avm_installed) {
      CheckUpgrade(envStatus.value.avm_version).then(info => { upgradeInfo.value = info })
    }
    setTimeout(() => { installResult.value = '' }, 3000)
  } catch {
    installResult.value = 'error'
  } finally {
    installing.value = false
  }
}

function openDownloadUrl(url: string) {
  window.open(url, '_blank')
}

function enterProject(p: Project) {
  projectStore.setCurrent(p.project_dir)
  settingsStore.save({ last_project: p.project_dir })
  router.push('/project/stats')
}

function onLangChange(e: Event) {
  const lang = (e.target as HTMLSelectElement).value
  locale.value = lang
  settingsStore.save({ language: lang })
  SetLanguage(lang).catch((err: Error) => console.error('SetLanguage failed:', err))
}

// Add project
function openAddModal() {
  addPath.value = ''
  browsing.value = false
  showAddModal.value = true
}

async function confirmAdd() {
  const path = addPath.value.trim()
  if (!path) {
    window.__toast?.show(t('pathRequired'), 'error')
    return
  }
  try {
    await projectStore.addProject(path)
    showAddModal.value = false
    window.__toast?.show(t('addProjectSuccess'), 'success')
  } catch (e: any) {
    window.__toast?.show(e?.message || 'Failed', 'error')
  }
}

async function selectDir() {
  try {
    const dir = await SelectDirectory()
    if (dir) addPath.value = dir
  } catch {}
}

async function openBrowser(path?: string) {
  try {
    const result = await BrowseDirectory(path || '')
    if (result) {
      browsing.value = true
      browseCurrentPath.value = result.path || ''
      browseDirsList.value = result.dirs || []
      addPath.value = result.path || ''
    }
  } catch {}
}

function browseUp() {
  const parent = browseCurrentPath.value.replace(/\/[^/]+\/?$/, '') || '/'
  openBrowser(parent)
}

function browseInto(dir: string) {
  const full = browseCurrentPath.value.replace(/\/$/, '') + '/' + dir
  addPath.value = full
  openBrowser(full)
}

// Delete project
function openDeleteModal(p: Project) {
  deleteTarget.value = p
  showDeleteModal.value = true
}

async function confirmDelete() {
  if (!deleteTarget.value) return
  try {
    await projectStore.deleteProject(deleteTarget.value.project_dir)
    showDeleteModal.value = false
    window.__toast?.show(t('opSuccess'), 'success')
  } catch (e: any) {
    window.__toast?.show(e?.message || 'Failed', 'error')
  }
}
</script>

<template>
  <div class="project-select">
    <!-- Language switcher -->
    <div class="lang-switcher--topright">
      <select class="lang-select" :value="locale" @change="onLangChange">
        <option v-for="(lang, code) in LANGS" :key="code" :value="code">{{ lang.label }}</option>
      </select>
    </div>

    <!-- Header -->
    <div class="project-select__header">
      <div class="project-select__logo">
        <svg class="project-select__logo-icon" viewBox="0 0 40 40">
          <line class="edge" x1="20" y1="8" x2="10" y2="28" />
          <line class="edge" x1="20" y1="8" x2="30" y2="28" />
          <line class="edge" x1="10" y1="28" x2="30" y2="28" />
          <circle class="node" cx="20" cy="8" r="4" />
          <circle class="node" cx="10" cy="28" r="4" />
          <circle class="node" cx="30" cy="28" r="4" />
        </svg>
        <span>AIVectorMemory</span>
      </div>
      <div class="project-select__title">{{ t('selectProject') }}</div>
    </div>

    <!-- Environment banner -->
    <div v-if="envStatus && !envStatus.python_found" class="env-banner env-banner--warning">
      <span>{{ t('envNoPython') }}</span>
      <a class="env-banner__link" href="https://www.python.org/downloads/" target="_blank">{{ t('envNoPythonLink') }}</a>
    </div>
    <div v-else-if="envStatus && !envStatus.avm_installed && !installing && installResult !== 'success'" class="env-banner env-banner--info">
      <span>{{ t('envNoPackage') }}</span>
      <button class="env-banner__btn" @click="handleInstall(false)">{{ t('envInstallBtn') }}</button>
    </div>
    <div v-else-if="installing" class="env-banner env-banner--info">
      <span class="env-banner__spinner" />
      <span>{{ t('envInstalling') }}</span>
    </div>
    <div v-else-if="installResult === 'success'" class="env-banner env-banner--success">
      <span>{{ t('envInstallSuccess') }}</span>
    </div>
    <div v-else-if="installResult === 'error'" class="env-banner env-banner--error">
      <span>{{ t('envInstallError') }}</span>
    </div>
    <template v-else-if="envStatus?.avm_installed && upgradeInfo">
      <div v-if="upgradeInfo.avm_update_available" class="env-banner env-banner--info">
        <span>{{ t('envAvmUpdate', { version: upgradeInfo.avm_latest }) }}</span>
        <button class="env-banner__btn" @click="handleInstall(true)">{{ t('envUpgradeBtn') }}</button>
      </div>
      <div v-if="upgradeInfo.app_update_available" class="env-banner env-banner--info">
        <span>{{ t('envAppUpdate', { version: upgradeInfo.app_latest }) }}</span>
        <button class="env-banner__btn" @click="openDownloadUrl(upgradeInfo.app_download_url)">{{ t('envDownloadBtn') }}</button>
      </div>
    </template>

    <!-- Project grid -->
    <div class="project-grid">
      <!-- Add card -->
      <div class="project-card project-card--add" @click="openAddModal">
        <div class="project-card__icon project-card__icon--add">
          <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <line x1="12" y1="5" x2="12" y2="19" /><line x1="5" y1="12" x2="19" y2="12" />
          </svg>
        </div>
        <div class="project-card__name">{{ t('addProject') }}</div>
      </div>

      <!-- Project cards -->
      <div
        v-for="(p, i) in projects"
        :key="p.project_dir"
        class="project-card"
        :style="{ animationDelay: `${(i + 1) * 0.05}s` }"
        @click="enterProject(p)"
      >
        <button class="project-card__delete" :title="t('deleteProjectBtn')" @click.stop="openDeleteModal(p)">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <polyline points="3 6 5 6 21 6" /><path d="M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2" />
          </svg>
        </button>
        <div class="project-card__icon">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z" />
          </svg>
        </div>
        <div class="project-card__name">{{ p.name }}</div>
        <div class="project-card__path">{{ p.project_dir }}</div>
        <div class="project-card__stats">
          <div class="project-card__stat">
            <div class="project-card__stat-num project-card__stat-num--blue">{{ (p.memories || 0) + (p.user_memories || 0) }}</div>
            <div class="project-card__stat-label">{{ t('memories') }}</div>
          </div>
          <div class="project-card__stat">
            <div class="project-card__stat-num project-card__stat-num--amber">{{ p.issues || 0 }}</div>
            <div class="project-card__stat-label">{{ t('issues') }}</div>
          </div>
          <div class="project-card__stat">
            <div class="project-card__stat-num project-card__stat-num--cyan">{{ p.tags || 0 }}</div>
            <div class="project-card__stat-label">{{ t('tags') }}</div>
          </div>
        </div>
      </div>

      <!-- Welcome guide when no projects -->
      <div v-if="projects.length === 0" class="welcome-guide" style="grid-column: 1 / -1">
        <div class="welcome-guide__title">{{ t('welcomeTitle') }}</div>
        <div class="welcome-guide__desc" v-html="t('welcomeDesc').replace(/\\n/g, '<br>')" />
        <div class="welcome-guide__cmd">{{ t('welcomeCmd') }}</div>
      </div>
    </div>

    <!-- Footer -->
    <div class="project-select__footer">
      {{ t('footer', { n: projects.length }) }}
    </div>

    <!-- Add project modal -->
    <Modal :show="showAddModal" :title="t('addProjectTitle')" :confirm-text="t('confirm')" @close="showAddModal = false" @confirm="confirmAdd">
      <div class="add-project-form">
        <label>{{ t('projectPath') }}</label>
        <div class="add-project-input-row">
          <input v-model="addPath" type="text" :placeholder="t('addProjectPlaceholder')" @keyup.enter="confirmAdd" />
          <button class="btn-browse" @click="selectDir">{{ t('browse') }}</button>
        </div>
        <div v-if="browsing" class="dir-browser">
          <div class="dir-browser__path">{{ browseCurrentPath }}</div>
          <div class="dir-browser__list">
            <div class="dir-browser__item dir-browser__item--parent" @click="browseUp">{{ t('parentDir') }}</div>
            <div v-for="d in browseDirsList" :key="d" class="dir-browser__item" @click="browseInto(d)">{{ d }}</div>
          </div>
        </div>
      </div>
    </Modal>

    <!-- Delete confirmation modal -->
    <Modal
      :show="showDeleteModal"
      :title="t('deleteProjectBtn')"
      :confirm-text="t('confirm')"
      danger
      @close="showDeleteModal = false"
      @confirm="confirmDelete"
    >
      <p style="line-height: 1.8; white-space: pre-line">{{ t('confirmDeleteProject', { name: deleteTarget?.name || '' }) }}</p>
    </Modal>
  </div>
</template>

<style scoped>
.project-select {
  width: 100%; min-height: 100vh; display: flex; flex-direction: column;
  align-items: center; justify-content: flex-start; padding: 60px 0;
  position: relative; overflow-y: auto;
}
.project-select::before {
  content: ''; position: fixed; top: -30%; left: -10%;
  width: 600px; height: 600px;
  background: radial-gradient(circle, rgba(59,130,246,0.08) 0%, transparent 70%);
  animation: floatOrb1 20s ease-in-out infinite; pointer-events: none;
}
.project-select::after {
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
.project-select__header { text-align: center; margin-bottom: 52px; position: relative; z-index: 1; }
.project-select__logo { display: inline-flex; align-items: center; gap: 12px; margin-bottom: 16px; }
.project-select__logo svg { width: 36px; height: 36px; }
.project-select__logo .node { fill: var(--accent); }
.project-select__logo .edge { stroke: var(--accent); stroke-width: 1.2; opacity: 0.5; }
.project-select__logo span { font-family: var(--font-mono); font-size: 22px; font-weight: 700; letter-spacing: -0.3px; }
.project-select__title { font-size: 15px; font-weight: 400; color: var(--text-muted); }

.project-grid {
  display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
  gap: 20px; width: 100%; max-width: 1000px; padding: 0 32px;
  position: relative; z-index: 1;
}

.project-card {
  background: var(--glass-bg); backdrop-filter: blur(20px); -webkit-backdrop-filter: blur(20px);
  border-radius: 16px; padding: 28px 24px; border: 1px solid var(--border-light);
  cursor: pointer; transition: all 0.3s cubic-bezier(0.4,0,0.2,1);
  position: relative; overflow: hidden;
  animation: cardIn 0.5s cubic-bezier(0.4,0,0.2,1) backwards;
}
.project-card::before {
  content: ''; position: absolute; top: 0; left: 0; right: 0; height: 2px;
  background: linear-gradient(90deg, var(--accent), #8B5CF6, #06B6D4); opacity: 0; transition: opacity 0.3s ease;
}
.project-card::after {
  content: ''; position: absolute; top: -50%; left: -50%; width: 200%; height: 200%;
  background: radial-gradient(circle at 30% 30%, var(--accent-bg-subtle) 0%, transparent 50%);
  pointer-events: none; opacity: 0; transition: opacity 0.3s ease;
}
.project-card:hover {
  border-color: var(--accent); transform: translateY(-4px);
  box-shadow: 0 12px 40px rgba(0,0,0,0.3), 0 0 0 1px var(--accent-bg-light), inset 0 1px 0 rgba(255,255,255,0.03);
}
.project-card:hover::before { opacity: 1; }
.project-card:hover::after { opacity: 1; }
.project-card__icon {
  width: 44px; height: 44px; border-radius: 12px;
  background: linear-gradient(135deg, var(--accent-bg), var(--color-purple-bg));
  display: flex; align-items: center; justify-content: center; margin-bottom: 18px;
}
.project-card__icon svg { width: 20px; height: 20px; color: var(--accent-light); }
.project-card__name { font-family: var(--font-mono); font-size: 15px; font-weight: 600; color: var(--text-primary); margin-bottom: 6px; }
.project-card__path { font-family: var(--font-mono); font-size: 11px; color: var(--text-dim); margin-bottom: 22px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.project-card__delete {
  position: absolute; top: 28px; right: 24px; z-index: 2;
  width: 28px; height: 28px; border: none; border-radius: 6px;
  background: transparent; color: var(--text-muted); cursor: pointer;
  display: flex; align-items: center; justify-content: center;
  opacity: 0; transition: all 0.15s ease;
}
.project-card:hover .project-card__delete { opacity: 1; }
.project-card__delete:hover { background: var(--color-error-bg); color: var(--color-error-light); }
.project-card__stats { display: flex; border-top: 1px solid var(--border-light); padding-top: 18px; }
.project-card__stat { flex: 1; text-align: center; position: relative; }
.project-card__stat + .project-card__stat::before {
  content: ''; position: absolute; left: 0; top: 2px; bottom: 2px; width: 1px; background: var(--border-light);
}
.project-card__stat-num { font-family: var(--font-mono); font-size: 22px; font-weight: 700; line-height: 1; margin-bottom: 4px; }
.project-card__stat-num--blue { color: var(--accent); }
.project-card__stat-num--cyan { color: var(--color-cyan); }
.project-card__stat-num--amber { color: var(--color-warning); }
.project-card__stat-label { font-size: 11px; color: var(--text-muted); letter-spacing: 0.3px; }
.project-select__footer { text-align: center; margin-top: 48px; font-size: 12px; color: var(--text-dim); position: relative; z-index: 1; }

.project-card--add {
  display: flex; flex-direction: column; align-items: center; justify-content: center;
  border: 2px dashed var(--color-purple-bg); background: transparent; min-height: 180px;
}
.project-card--add:hover { border-color: var(--color-purple); background: var(--bg-hover); }
.project-card__icon--add { background: linear-gradient(135deg, var(--color-purple-bg), var(--color-purple-bg)); }
.project-card__icon--add svg { color: var(--color-purple); }

/* Add project form */
.add-project-form label { display: block; font-size: 13px; color: var(--text-secondary); margin-bottom: 8px; }
.add-project-input-row { display: flex; gap: 8px; }
.add-project-input-row input {
  flex: 1; background: var(--bg-input); border: 1px solid var(--border-light); border-radius: var(--radius);
  padding: 10px 14px; color: var(--text-primary); font-family: var(--font-mono); font-size: 13px; outline: none;
}
.add-project-input-row input:focus { border-color: var(--color-purple); }
.btn-browse {
  background: var(--color-purple-bg); color: var(--color-purple); border: 1px solid var(--color-purple-bg);
  border-radius: var(--radius); padding: 10px 16px; cursor: pointer; font-size: 13px; white-space: nowrap;
  transition: all var(--transition-fast);
}
.btn-browse:hover { background: hsl(270 76% 60% / 0.25); border-color: var(--color-purple); }

.dir-browser { margin-top: 12px; background: var(--bg-input); border: 1px solid var(--border-light); border-radius: var(--radius); overflow: hidden; }
.dir-browser__path { padding: 8px 14px; font-family: var(--font-mono); font-size: 11px; color: var(--text-muted); border-bottom: 1px solid var(--border-light); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.dir-browser__list { max-height: 240px; overflow-y: auto; }
.dir-browser__item { padding: 8px 14px; font-size: 13px; color: var(--text-heading); cursor: pointer; transition: background 0.1s; display: flex; align-items: center; gap: 6px; }
.dir-browser__item:hover { background: var(--color-purple-bg); }
.dir-browser__item--parent { color: var(--color-purple); font-size: 12px; border-bottom: 1px solid var(--border-light); }

/* Welcome guide */
.welcome-guide { text-align: center; padding: 48px 32px; max-width: 520px; margin: 0 auto; }
.welcome-guide__title { font-family: var(--font-mono); font-size: 16px; font-weight: 600; color: var(--text-heading); margin-bottom: 12px; }
.welcome-guide__desc { font-size: 13px; color: var(--text-muted); line-height: 1.8; margin-bottom: 8px; }
.welcome-guide__cmd {
  font-family: var(--font-mono); font-size: 12px; color: var(--color-purple);
  background: var(--color-purple-bg); padding: 8px 16px; border-radius: var(--radius);
  display: inline-block; margin-top: 12px; user-select: all;
}

/* Environment banner */
.env-banner {
  width: 100%; max-width: 1000px; padding: 12px 24px; margin-bottom: 20px;
  border-radius: 10px; display: flex; align-items: center; gap: 12px;
  font-size: 13px; position: relative; z-index: 1;
  animation: cardIn 0.3s ease backwards;
}
.env-banner--warning { background: hsl(45 80% 50% / 0.12); border: 1px solid hsl(45 80% 50% / 0.25); color: hsl(45 80% 70%); }
.env-banner--info { background: var(--accent-bg); border: 1px solid var(--accent-bg-light); color: var(--accent-light); }
.env-banner--success { background: hsl(142 50% 45% / 0.12); border: 1px solid hsl(142 50% 45% / 0.25); color: hsl(142 50% 70%); }
.env-banner--error { background: hsl(0 70% 50% / 0.12); border: 1px solid hsl(0 70% 50% / 0.25); color: hsl(0 70% 70%); }
.env-banner__link {
  color: inherit; text-decoration: underline; cursor: pointer; font-weight: 500;
}
.env-banner__btn {
  margin-left: auto; padding: 6px 16px; border-radius: 6px; border: none;
  font-size: 12px; font-weight: 600; cursor: pointer; white-space: nowrap;
  background: var(--accent); color: #fff; transition: opacity 0.15s;
}
.env-banner__btn:hover { opacity: 0.85; }
.env-banner--warning .env-banner__btn { background: hsl(45 80% 50%); color: #1a1a2e; }
.env-banner__spinner {
  width: 16px; height: 16px; border: 2px solid var(--accent-light); border-top-color: transparent;
  border-radius: 50%; animation: spin 0.8s linear infinite; flex-shrink: 0;
}
@keyframes spin { to { transform: rotate(360deg); } }

/* Language switcher */
.lang-switcher--topright { position: absolute; top: 20px; right: 32px; z-index: 10; }
.lang-select {
  padding: 6px 28px 6px 10px; border: 1px solid var(--border); border-radius: var(--radius);
  font-size: 13px; color: var(--text-primary); background: var(--bg-surface); font-family: inherit;
  cursor: pointer; transition: all var(--transition-fast);
  appearance: none; -webkit-appearance: none;
  background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 24 24' fill='none' stroke='%2394A3B8' stroke-width='2'%3E%3Cpolyline points='6 9 12 15 18 9'/%3E%3C/svg%3E");
  background-repeat: no-repeat; background-position: right 8px center;
}
.lang-select:focus { outline: none; border-color: var(--accent); box-shadow: var(--shadow-focus); }
.lang-select:hover { border-color: var(--border-hover); }
.lang-select option { background: var(--bg-surface); color: var(--text-primary); }
</style>
