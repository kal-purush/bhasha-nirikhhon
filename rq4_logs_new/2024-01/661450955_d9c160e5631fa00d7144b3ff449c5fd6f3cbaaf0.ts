import { FastifyReply, FastifyRequest } from 'fastify'
import { bodySchema } from './schemas'
import { CreateUserService } from '../../services/user/createUserService'

export class CreatPostController {
  async handle(request: FastifyRequest, reply: FastifyReply) {
    const { authorId, published, content, title } = bodySchema.parse(
      request.body,
    )
    const post = new CreateUserService().execute({
      title,
      authorId,
      content,
      published,
    })

    return reply.status(201).send(post)
  }
}