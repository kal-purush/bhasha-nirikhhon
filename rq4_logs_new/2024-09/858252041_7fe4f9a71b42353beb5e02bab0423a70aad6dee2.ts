import { FastifyInstance } from "fastify";
import fp from "fastify-plugin";
import { Server } from "socket.io";
import { verifyToken } from "../lib/verifyToken.js";

declare module "fastify" {
  interface FastifyInstance {
    io: Server;
  }
}

export default fp(async (fastify, opts) => {
  const io = new Server(fastify.server, {
    ...opts,
    cors: { origin: "http://localhost:3001" },
  });

  io.use(async (socket, next) => {
    const token = socket.handshake.headers.authorization;
    const isValid = await verifyToken(token);
    if (!isValid) {
      const err = new Error("Unauthorized");
      next(err);
    } else {
      next();
    }
  });

  fastify.decorate("io", io);

  fastify.addHook("onClose", (fastify: FastifyInstance, done) => {
    (fastify as any).io.close();
    done();
  });
});