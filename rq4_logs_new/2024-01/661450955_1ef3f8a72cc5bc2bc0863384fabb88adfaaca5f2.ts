import { DeletePostServise } from '../../services/posts/deletePostService'
import { FastifyReply, FastifyRequest } from 'fastify'
import { PostSchema } from './schemas'

export class DeletePostController {
  async handle(request: FastifyRequest, reply: FastifyReply) {
    const { id } = PostSchema.parse(request.body)
    const { userId } = PostSchema.parse(request.params)

    new DeletePostServise().execute({ id, userId })

    return reply.status(204).send({ message: 'Post deletado com sucesso' })
  }
}