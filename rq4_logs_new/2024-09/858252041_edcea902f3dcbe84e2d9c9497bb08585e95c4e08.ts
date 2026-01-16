import { FastifyInstance } from "fastify";
import fp from "fastify-plugin";
import { Server } from "socket.io";

declare module "fastify" {
  interface FastifyInstance {
    io: Server;
  }
}

export default fp(async (fastify, opts) => {
  fastify.decorate(
    "io",
    new Server(fastify.server, {
      ...opts,
      cors: { origin: "http://localhost:3001" },
    }),
  );

  fastify.addHook("onClose", (fastify: FastifyInstance, done) => {
    (fastify as any).io.close();
    done();
  });
});