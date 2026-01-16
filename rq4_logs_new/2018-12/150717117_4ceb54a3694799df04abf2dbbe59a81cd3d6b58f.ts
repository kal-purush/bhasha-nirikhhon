import { verify } from 'jsonwebtoken'
import { ValidationError } from 'yup'

import { Prisma } from '../generated/prisma-client'
import { Context, Token } from '../types/graphql-utils'

export function getUser(ctx: Context, prisma: Prisma) {
  const { authorization } = ctx.req.headers
  if (authorization) {
    const token = authorization.replace('Bearer ', '')
    const { userId: id } = verify(token, process.env.APP_SECRET) as Token
    return { id }
  }
}

export function sendError(err?: ValidationError) {
  const errors: Array<{ path: string, message: string }> = []
  if (err) {
    err.inner.forEach(({ path, message }) => errors.push({ path, message }))
  } else {
    errors.push({ path: '', message: 'Not authorized' })
  }

  return { errors }
}