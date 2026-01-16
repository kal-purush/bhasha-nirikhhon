import { Webhook } from 'discord-webhook-node'
import * as dotenv from 'dotenv'
import express from 'express'
import { z } from 'zod'

dotenv.config()
const hook = new Webhook(process.env.WEBHOOK_URL || '')
const app = express()
app.use(express.json())

const ReportBodySchema = z.object({
  type: z.union([z.literal('manual'), z.literal('automatic')]),
  message: z.string(),
  extVersion: z.string(),
})

app.post('/', (req, res) => {
  try {
    const body = ReportBodySchema.parse(req.body)
    hook.setUsername('WNPRedux Error Reporter')
    hook.setAvatar(process.env.WEBHOOK_AVATAR_URl || '')
    hook.send(`**Type:** ${body.type}\n**Message:** ${body.message}\n**Version:** ${body.extVersion}`)
    res.status(200).send('OK')
  } catch (e) {
    console.log(e)
    res.status(400).send('Bad Request')
  }
})

app.listen(process.env.PORT, () => console.log(`WNPRedux-Error-Proxy listening on port ${process.env.PORT}`))