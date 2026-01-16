import { FastifyInstance, FastifyPluginCallback } from 'fastify';
import fp from 'fastify-plugin';

const repositoriesPlugin: FastifyPluginCallback<{
  services: Record<string, FastifyService>;
  name: keyof FastifyInstance;
}> = (app, config, done) => (
  app.decorate(config.name, config.services),
  Object.values(app[config.name]).forEach(service => service.register(app)),
  done()
);

export const registerServices = fp(repositoriesPlugin);

export interface FastifyService {
  register(app: FastifyInstance): void;
}