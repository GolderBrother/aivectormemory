/// <reference types="vite/client" />

declare module '*.vue' {
    import type {DefineComponent} from 'vue'
    const component: DefineComponent<{}, {}, any>
    export default component
}

interface Window {
    __toast?: { show: (text: string, type?: string) => void }
}
