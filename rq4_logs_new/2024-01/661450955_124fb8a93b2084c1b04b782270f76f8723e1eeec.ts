import fastify from 'fastify'
import multipart from '@fastify/multipart'
import { Users } from './routes/users'
import { Posts } from './routes/posts'
import { Upload } from './routes/upload'
import { ErrorControllers } from './middleware/errors'
import { FastifyInstance } from 'fastify/types/instance'

// import { resolve } from 'node:path'
// import { fastifyStatic } from '@fastify/static'

// const errorControllers = new ErrorControllers()

export class App {
  public readonly server: FastifyInstance
  public errorControllers: ErrorControllers

  constructor() {
    this.server = fastify()
    this.errorControllers = new ErrorControllers()

    this.server.register(multipart)
    this.server
      .register(Users, {
        prefix: 'users',
      })
      .register(Posts, {
        prefix: 'posts',
      })
      .register(Upload, {
        prefix: 'upload',
      })
      .after(() => {
        console.log(this.server.printRoutes())
      })
    this.server.setErrorHandler(this.errorControllers.getUp)
  }
}