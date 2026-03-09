import { createRouter, createWebHashHistory } from 'vue-router'
import { useAuthStore } from '../stores/auth'

const router = createRouter({
  history: createWebHashHistory(),
  routes: [
    { path: '/auth', name: 'auth', component: () => import('../views/AuthView.vue'), meta: { guest: true } },
    { path: '/', name: 'projects', component: () => import('../views/ProjectSelect.vue') },
    {
      path: '/project',
      component: () => import('../views/ProjectLayout.vue'),
      children: [
        { path: 'stats', name: 'stats', component: () => import('../views/StatsView.vue') },
        { path: 'status', name: 'status', component: () => import('../views/StatusView.vue') },
        { path: 'issues', name: 'issues', component: () => import('../views/IssuesView.vue') },
        { path: 'tasks', name: 'tasks', component: () => import('../views/TasksView.vue') },
        { path: 'project-memories', name: 'project-memories', component: () => import('../views/MemoriesView.vue'), props: { scope: 'project' } },
        { path: 'user-memories', name: 'user-memories', component: () => import('../views/MemoriesView.vue'), props: { scope: 'user' } },
        { path: 'tags', name: 'tags', component: () => import('../views/TagsView.vue') },
        { path: 'settings', name: 'settings', component: () => import('../views/SettingsView.vue') },
      ],
    },
  ],
})

router.beforeEach((to, _from, next) => {
  const authStore = useAuthStore()
  if (to.meta.guest) return next()
  if (!authStore.isLoggedIn) return next('/auth')
  next()
})

export default router
