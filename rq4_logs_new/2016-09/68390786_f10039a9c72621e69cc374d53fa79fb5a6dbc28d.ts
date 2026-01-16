import { createServer } from 'http'

export let LISTEN = (a1, a2?, a3?) => {

  let handler = (router, port, host = null, cb?) => {
    let name = router.__base_class.name || ''
    let defaultHandler = (err) => {
      if (err) return console.error('failed')
      console.info(`[${new Date().toLocaleTimeString()}] ${name}#${process.pid} Listening ${host || ''}:${port}`)
    }
    let server = createServer(router).listen(port, host, cb || defaultHandler)
    return server
  }

  if (typeof a1 === 'function') {
    let {PORT, HOST} = process.env
    if (!PORT) {
      throw new Error('no PORT specified in process.env')
    }
    handler(a1, PORT, HOST)
    return a1
  } else {
    return (router) => {
      handler(router, a1, a2, a3)
    }
  }
}