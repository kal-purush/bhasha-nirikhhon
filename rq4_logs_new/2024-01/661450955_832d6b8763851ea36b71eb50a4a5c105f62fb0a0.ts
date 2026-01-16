import { FastifyReply, FastifyRequest } from 'fastify'
import { userSchemaId } from './schema'
import { DeleteUserService } from '../../services/users/deleteUserService'

export class DeleteUserControllers {
  async handle(request: FastifyRequest, reply: FastifyReply) {
    const { id } = userSchemaId.parse(request.params)

    const deleteUserService = new DeleteUserService()

    await deleteUserService.execute({ id })

    return reply.status(200).send({ message: 'User deleted with success' })
  }
}