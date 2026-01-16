import { zValidator } from '@hono/zod-validator'
import { Hono } from 'hono'
import { z } from 'zod'

const app = new Hono()
export const schema = z.object({
  title: z.coerce.string().trim().min(1).max(50),
  // dueDate: z.date(),
  // isDone: z.boolean()
  dueDate: z.string().datetime(),
  isDone: z.string()
})

export type Schema = z.infer<typeof schema>

const route = app.post('/todos', zValidator('form', schema), (c) => {
  return c.jsonT(
    {
      ok: true,
      message: 'Created!'
    },
    201
  )
})

export type AppType = typeof route