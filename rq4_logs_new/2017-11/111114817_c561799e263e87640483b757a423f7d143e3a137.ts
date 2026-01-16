import * as KoaRouter from 'koa-router'

export const Controller = (router: KoaRouter) => target => {
  target.prototype.routes.forEach(({ handler, method, path }) => router[method](path, handler))
}

export enum Method {
  ALL,
  DELETE,
  GET,
  HEAD,
  OPTIONS,
  PATCH,
  POST,
  PUT
}

const MethodMap = {
  [Method.ALL]: 'all',
  [Method.DELETE]: 'del',
  [Method.GET]: 'get',
  [Method.HEAD]: 'head',
  [Method.OPTIONS]: 'options',
  [Method.PATCH]: 'patch',
  [Method.POST]: 'post',
  [Method.PUT]: 'put',
  [Method.HEAD]: 'head'
}

export interface RequestMap {
  consumes?: string[]
  headers?: string[]
  method?: Method
  path: string
}

function RequestMapping(requestMap: RequestMap)
function RequestMapping(path: string, method?: Method)
function RequestMapping(path: string | RequestMap, method: Method = Method.GET) {
  let requestMap
  if (typeof path === 'string') {
    requestMap = {
      method,
      path
    }
  } else if (method !== undefined) {
    throw new TypeError()
  }
  return (target, propertyKey: string, descriptor: PropertyDescriptor) => {
    if (propertyKey) {
      if (!target.routes) {
        target.routes = []
      }

      target.routes.push({
        handler: descriptor.value,
        method: MethodMap[requestMap.method],
        path: requestMap.path
      })
    }
  }
}

export { RequestMapping }