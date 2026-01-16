import { type NextRequest } from 'next/server'
import { envAllowList } from './envAllowList'
import { type EnvAllowList } from 'types/envAllowlist'

export function GET(request: NextRequest) {
  const entries = request.nextUrl.searchParams.entries()

  for (const [key] of entries) {
    if (!envAllowList.includes(key as EnvAllowList))
      return Response.json({ message: 'invalid request' })
    return Response.json({ [key]: process.env[key] })
  }

  return Response.json({ message: 'searchParams missing' })
}