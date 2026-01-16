import { 
  FastifyInstance, 
  FastifyPluginOptions, 
  FastifyError
} from 'fastify';
import fp from 'fastify-plugin';
import usersController from '../controllers/usersController';
import { UserParams } from '../models/usersModel';

function UsersRoutes(
    fastify: FastifyInstance,
    options: FastifyPluginOptions,
    next: (err?: FastifyError) => void
){
    fastify.get<{ Params: UserParams }>('/users/:id', usersController.showById);
    fastify.delete<{ Params: UserParams }>('/users/:id', usersController.deleteById);
    next();
}

export default fp(UsersRoutes);