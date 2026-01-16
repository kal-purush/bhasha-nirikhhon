import { object, string } from 'yup'

import { sendError } from './utils'
import { Context } from '../types/graphql-utils'

const yupSchema = object().shape({
  name: string().trim().min(2).max(255),
  email: string().trim().min(3).max(255).email(),
  password: string().min(3).max(255)
})

export const validation = {
  async Mutation (resolve: any, root: any, args: object, ctx: Context, info: any) {
    try {
      await yupSchema.validate(args, { abortEarly: false })
    } catch (err) {
      return sendError(err)
    }
    const result = await resolve(root, args, ctx, info)
    return result
  }
}