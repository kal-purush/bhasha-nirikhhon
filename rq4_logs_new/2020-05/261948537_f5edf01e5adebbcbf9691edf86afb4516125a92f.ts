import WebSocket from 'ws'
import { Context } from '../context'
import debug from 'debug'
const logger = debug('pnut-stream:client')

function getStreamUrl(appAccessToken: string, appStreamKey: string) {
  return `wss://stream.pnut.io/v0/app?access_token=${appAccessToken}&key=${appStreamKey}&include_raw=1`
}

export function createClientInstance(context: Context) {
  return (appStreamKey: string) => {
    const ws = new WebSocket(
      getStreamUrl(context.env.appAccessToken, appStreamKey)
    )
    const connection = setEventHandler(ws)
    return {
      ws,
      connection,
    }
  }
}

function setEventHandler(ws: WebSocket): Promise<never> {
  ws.on('open', () => {
    logger('client socket is open')
  })
  ws.on('message', (data) => {
    logger(data)
  })

  const connection = new Promise<never>((resolve, reject) => {
    ws.on('close', (code, reason) => {
      logger(`connection closed. code: ${code}, reason: ${reason}`)
      resolve()
    })
    ws.on('error', (err) => {
      logger(err)
      reject(err)
    })
    ws.on('unexpected-response', () => {
      logger('unexpected-response')
      reject()
    })
  })
  return connection
}