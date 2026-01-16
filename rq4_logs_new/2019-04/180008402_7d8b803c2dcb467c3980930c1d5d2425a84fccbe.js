import ContentRoute from './content-route.js'
import notImplemented from './utils/not-implemented.js'

export default class ContentApplication extends window.EventTarget {
  constructor () {
    super()

    this.context = {
      engines: {},
      files: {},
      routes: [],
      settings: {},
      views: {}
    }

    this.mountPath = '/'

    Object.assign(this, {
      all: notImplemented('application.all(path, callback)'),
      disable: notImplemented('application.disable(name)'),
      disabled: notImplemented('application.disabled(name)'),
      enable: notImplemented('application.enable(name)'),
      enabled: notImplemented('application.enabled(name)'),
      listen: notImplemented('application.listen(port, host, backlog, callback)'),
      METHOD: notImplemented('application.METHOD(path, callback, ...callbacks)'),
      param: notImplemented('application.param(name, callback)')
    })
  }
  delete (path, callback) {
    this.context.routes.push(new ContentRoute('DELETE', path, callback))

    return this
  }
  engine (ext, callback) {
    this.context.engines[ext] = callback

    return this
  }
  get (path, callback) {
    if (!callback) {
      return this.context.settings[path]
    }

    this.context.routes.push(new ContentRoute('GET', path, callback))

    return this
  }
  path () {
    return this.mountPath
  }
  post (path, callback) {
    this.context.routes.push(new ContentRoute('POST', path, callback))

    return this
  }
  put (path, callback) {
    this.context.routes.push(new ContentRoute('PUT', path, callback))

    return this
  }
  render (path, callback) {
    this.context.engines.html(path)
      .then(html => callback(null, html))

    return this
  }
  route (path, body) {
    let result = {}

    this.context.routes.some(route => {
      const params = route.match(path, body)

      if (params) {
        result.callback = route.callback
        result.route = route.path
        result.params = params

        return true
      }
    })

    if (result.route) {
      return result
    }
  }
  set (name, value) {
    this.context.settings[name] = value

    return this
  }
  use (path, app) {
    app.mountPath = path

    return this
  }
}