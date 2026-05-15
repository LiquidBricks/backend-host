export async function runHandler(handler, { rootCtx, scope } = {}) {
  if (typeof handler === 'function') {
    return handler({ rootCtx, scope })
  }

  if (!Array.isArray(handler)) {
    throw new TypeError('handler must be a function or an array of functions')
  }

  const mutableScope = { ...(scope ?? {}) }
  for (const fn of handler) {
    if (typeof fn !== 'function') continue
    const result = await fn({ rootCtx, scope: mutableScope })
    if (result && typeof result === 'object') Object.assign(mutableScope, result)
  }
  return mutableScope
}
