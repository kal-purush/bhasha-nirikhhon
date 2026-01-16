import * as express from 'express'
import { syncHandler } from './utils'
import { DocumentType } from '@typegoose/typegoose'
import UserModel, { User } from '../models/user'
import { parseToken } from '../models/auth'

type AuthteticatedRequest = express.Request & { user: DocumentType<User> }


export const tokenAuth = syncHandler(async (req: AuthteticatedRequest, res, next) => {
  const queryparams = req.query as any
  const { username } = parseToken(queryparams['access_token'])
  const user = await UserModel.findByUsername(username).exec()
  if (!user) throw "Fallo de sesión"
  req.user = user
  next()
})

export const requiredBody = (...fields: string[]) => syncHandler(async (req: AuthteticatedRequest, res, next) => {
  // TODO
  // if (!req.body) return next(new BodyNotFound())
  // if (!fields.every(field => req.body[field])) return next(new BodyParametersNotFound(...fields.filter(field => !req.body[field])))
  next()
})