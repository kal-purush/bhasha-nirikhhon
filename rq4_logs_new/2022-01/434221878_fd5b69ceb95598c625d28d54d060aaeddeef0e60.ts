// login.router.ts

import HTTP_STATUS from 'http-status';

import fastify from 'fastify';

import Logger from '../../common/logger';

import { DatabaseConnection } from '../../db/database';

import Login from './login.model';
import { ILogin } from './login.types';
import LoginService from './login.service';

/**
 * Fastify server instance.
 *
 * TODO: this declaration is repeated in several files.
 */
type Server = ReturnType<typeof fastify>;

/**
 * Router object for `login` endpoint.
 */
class LoginRouter {
  log: Logger;

  fastify: Server;

  service: LoginService;

  constructor(log: Logger, server: Server, db: DatabaseConnection) {
    this.log = log;
    this.fastify = server;
    this.service = new LoginService(log, db);

    this.fastify.post<{ Body: ILogin }>('/login', {
      handler: async (q, p) => {
        const { status, payload } = await this.service.auth(q);
        await p.code(status).send(payload);
      },
      schema: {
        tags: Login.schema.tags,
        body: Login.schema.request,
        response: {
          [HTTP_STATUS.CREATED]: Login.schema.response,
        },
      },
    });
  }
}

export default LoginRouter;

// __EOF__