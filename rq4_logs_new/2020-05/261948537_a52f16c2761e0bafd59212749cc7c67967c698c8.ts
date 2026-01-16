import { init } from 'core/init'
import { createClientInstance } from 'client'
import debug from 'debug'
import { tearDown } from 'core/tearDown'
import { Context } from 'context'
import WebSocket from 'ws'
const logger = debug('pnut-stream:core:main')

export async function main() {
  const { context, key } = await init()
  for (;;) {
    const { ws, connection } = createClientInstance(context)(key)
    try {
      const removeListeners = setProcessEventListeners(context, ws)
      await connection
      removeListeners()
      logger('closed, reconnecting...')
    } catch (e) {
      logger('terminated:', e)
      break
    }
  }
}

function setProcessEventListeners(context: Context, ws: WebSocket) {
  const shutdown = async () => {
    logger('graceful shutdown...')
    await tearDown(context, ws)()
  }
  process.on('SIGINT', shutdown)
  process.on('SIGTERM', shutdown)
  return () => {
    process.off('SIGINT', shutdown)
    process.off('SIGTERM', shutdown)
  }
}