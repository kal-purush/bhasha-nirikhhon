import { parse } from 'url'

export let bindContainer = (router, cfg) => {
  if (!cfg) return
  if (cfg.option && cfg.option.middlewares) {
    router.use(cfg.option.middlewares)
  }
  if (cfg.param) {
    for (let t in cfg.param) {
      router.param(t, cfg.param[t])
    }
  }
}

let msHandler = (getQuery, handler) => async(req, res, next) => {
  let query = getQuery(req)
  let ended = false
  res.on('end', () => ended = true)
  let result = await handler(query, req, res, next)
  if (!ended) {
    res.json(result)
  }
}

let patternHandler = (getQuery, handler, path) => async(req, res, next) => {
  let query = getQuery(req)
  let keys = Object.keys(query)
  keys.splice(keys.indexOf('arg'), 1)
  let isPattern = keys.every(k => query[k] === path[k])
  if (!isPattern) return next()
  let ended = false
  res.on('end', () => ended = true)
  let {arg = {}} = query
  let result = await handler(arg, req, res, next)
  if (!ended) {
    res.json(result)
  }
}

export let bindRoute = (router, cfg) => {
  if (!cfg) return

  if (cfg.route) {
    for (let t in cfg.route) {
      let item = cfg.route[t]
      let args, handler, method
      switch (item.method) {
        case '_msqs':
          handler = msHandler((req) => parse(req.url, true).query, item.handler)
          method = 'get'
          args = [item.path, ...item.middlewares, handler]
          router.get.apply(router, args)
          break
        case '_msbd':
          handler = msHandler((req) => req.body, item.handler)
          method = 'post'
          args = [item.path, ...item.middlewares, handler]
          router.post.apply(router, args)
          break
        case '_pattern':
          handler = patternHandler((req) => req.body, item.handler, item.path)
          method = 'post'
          args = ['*', ...item.middlewares, handler]
          router.post.apply(router, args)
          break
        default:
          args = [item.path, ...item.middlewares, item.handler]
          router[item.method].apply(router, args)
      }
    }
  }
}

export let bindContainerProps = (router, props) => {
  if (!props) return
  for (let k in props) {
    switch (k) {
      case 'set':
        for (let s in props.set) {
          router.set(s, props.set[s])
        }
        break
      case 'locals', 'mountpath':
        router[k] = props[k]
        break
      // not to provide listen method to probably confuse others
    }
  }
}