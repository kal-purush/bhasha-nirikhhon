import { Context } from 'hono'

export function CACHE() {
  return caches.open('sticker-api:cache')
}

export async function cacheMatch<T extends string>(
  ctx: Context<T, Env>,
  options?: {
    path?: string
  },
) {
  const cache = await CACHE()
  const { origin, pathname } = new URL(ctx.req.url)
  const cacheURL = new URL(options?.path || pathname, origin)
  const cacheKey = cacheURL.toString()
  return { res: await cache.match(cacheKey), key: cacheKey, store: cache }
}