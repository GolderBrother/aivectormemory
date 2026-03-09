export function showToast(text: string, type: 'success' | 'error' | 'warning' | 'info' = 'success') {
  const t = (window as any).__toast
  if (t) t.show(text, type)
}

export function showError(e: unknown) {
  const msg = e instanceof Error ? e.message : String(e)
  showToast(msg, 'error')
}
