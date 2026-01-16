import fastify from 'fastify'
import { redisClient } from 'src/redis'
import fetch from 'node-fetch'
import { PnutResposne } from 'src/models/pnutResponse'
import { Token } from 'src/models/token'

interface Body {
  pnutAccessToken: string
  fcmToken: string
}

function createApp() {
  const app = fastify({ logger: true })
  // const opts
  app.post<
    fastify.DefaultQuery,
    fastify.DefaultParams,
    fastify.DefaultHeaders,
    Body
  >('/', async (req, res) => {
    const { pnutAccessToken, fcmToken } = req.body
    if (!pnutAccessToken || !fcmToken)
      throw new Error('payload must have id and token.')
    const id = await verifyPnutToken(pnutAccessToken)
    app.log.info(`id: ${id}, token: ${fcmToken}`)
    redisClient.add(id, fcmToken)
    res.code(201).send()
  })
  return app
}

async function verifyPnutToken(pnutAccessToken: string): Promise<string> {
  const res: PnutResposne<Token> = await fetch('https://api.pnut.io/v0/token', {
    headers: {
      Authorization: `Bearer ${pnutAccessToken}`,
    },
  }).then((res) => res.json())
  return res.data.user.id
}

async function main() {
  const app = createApp()
  const address = await app.listen(+(process.env.PORT || '') || 3000)
  app.log.info(`server listening on ${address}`)
}

main()