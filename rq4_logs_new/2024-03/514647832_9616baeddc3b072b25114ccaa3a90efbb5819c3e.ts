import { eq } from 'drizzle-orm'
import { drizzle } from 'drizzle-orm/d1'
import type { Logger } from 'drizzle-orm/logger'
import { narou } from '~/datasource/d1/schema'

class D1Logger implements Logger {
  logQuery(query: string, params: unknown[]): void {
    console.info(`d1 sql : query='${query}' params='${params}'`)
  }
}

export const listAll = async (d1: D1Database) => {
  const db = drizzle(d1, { schema: { narou }, logger: new D1Logger() })
  const data = await db.query.narou.findMany()
  return data
}

type GetNarouNovelParams = {
  ncode: string
}
export const getOne = async (d1: D1Database, params: GetNarouNovelParams) => {
  const db = drizzle(d1, { schema: { narou }, logger: new D1Logger() })
  const result = await db.query.narou.findFirst({ where: eq(narou.ncode, params.ncode) })
  return result
}

type CreateNarouNovelParams = {
  ncode: string
  currentPage: number
}
export const insert = async (d1: D1Database, params: CreateNarouNovelParams) => {
  const db = drizzle(d1, { schema: { narou }, logger: new D1Logger() })
  const result = await db.insert(narou).values(params)
  return result
}

type DeleteNarouNovelParams = {
  ncode: string
}
export const remove = async (d1: D1Database, params: DeleteNarouNovelParams) => {
  const db = drizzle(d1, { schema: { narou }, logger: new D1Logger() })
  const result = await db.delete(narou).where(eq(narou.ncode, params.ncode))
  return result
}

type UpdateNarouNovelParams = {
  ncode: string
  currentPage: number
}
export const update = async (d1: D1Database, params: UpdateNarouNovelParams) => {
  const db = drizzle(d1, { schema: { narou }, logger: new D1Logger() })
  const result = await db
    .update(narou)
    .set({ currentPage: params.currentPage })
    .where(eq(narou.ncode, params.ncode))
  return result
}