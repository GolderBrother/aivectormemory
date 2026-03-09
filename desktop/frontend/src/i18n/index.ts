import { createI18n } from 'vue-i18n'
import zhCN from './zh-CN'
import zhTW from './zh-TW'
import en from './en'
import es from './es'
import de from './de'
import fr from './fr'
import ja from './ja'

export const LANGS = {
  'zh-CN': { label: '简体中文' },
  'zh-TW': { label: '繁體中文' },
  'en':    { label: 'English' },
  'es':    { label: 'Español' },
  'de':    { label: 'Deutsch' },
  'fr':    { label: 'Français' },
  'ja':    { label: '日本語' },
}

const i18n = createI18n({
  legacy: false,
  locale: 'zh-CN',
  fallbackLocale: 'zh-CN',
  messages: { 'zh-CN': zhCN, 'zh-TW': zhTW, en, es, de, fr, ja },
})

export default i18n
