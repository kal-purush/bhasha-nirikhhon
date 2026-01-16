import { hc } from 'hono/client'
import type { AppType, TodoResponse } from '@server/model'

const client = hc<AppType>('http://localhost:8788/api')

// export const post = await client.todos.$post({
//   form: {
//     title: 'aaa',
//     isDone: String(false)
//   }
// })

/**
 * @desc Todo全件取得
 */
export async function getAllTodo(): Promise<{
  ok: boolean
  todos: TodoResponse[]
}> {
  const res = await client.todos.$get()
  // https://github.com/honojs/hono/issues/950
  return (await res.json()) as unknown as {
    ok: boolean
    todos: TodoResponse[]
  }
}