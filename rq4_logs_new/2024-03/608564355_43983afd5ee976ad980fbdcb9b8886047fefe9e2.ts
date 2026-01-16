import type { Meta } from '../meta'

export function resolveDep(ret: any, key: string) {
  if (key)
    return ret?.[key]
  return ret
}

export function argToReq(params: Meta['data']['params'], args: any[], headers: Record<string, any>) {
  const req = {
    body: {},
    query: {},
    params: {},
    headers,
  } as any

  params.forEach((param) => {
    if (param.key)
      req[param.type][param.key] = args[param.index]

    else
      req[param.type] = args[param.index]
  })

  return req
}