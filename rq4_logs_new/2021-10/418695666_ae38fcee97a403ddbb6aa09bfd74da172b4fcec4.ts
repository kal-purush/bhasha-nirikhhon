import * as server from './server'

export async function start(): Promise<void> {
  server.connect()
}

export async function stop(): Promise<void> {
  await server.disconnect()
}