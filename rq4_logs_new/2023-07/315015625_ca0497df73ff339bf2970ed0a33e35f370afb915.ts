// This in-memory "cache" serves to speed-up local development

const InMemory = new Map();

export function set(key: string, value: any, cache = InMemory): any {
  cache.set(key, value);
}

export async function proxyCache(
  key: string,
  executor: () => any,
  cache = InMemory
) {
  if (cache.has(key)) {
    return cache.get(key);
  }
  const result = await executor();
  set(key, result);
  return result;
}