import fp from "fastify-plugin";
import {
  FastifyPluginOptions,
  FastifyInstance,
  FastifyRequest,
  FastifyReply,
} from "fastify";
import type {PublicUserType} from "./../models/User"

function jwtPlugin(fastify: FastifyInstance, _options: FastifyPluginOptions,done:any) {
  fastify.register(require("@fastify/jwt"), {
    secret: "supersecret",
  });

  fastify.decorate(
    "authenticate",
    async (request: FastifyRequest, reply: FastifyReply) => {
      try {
        const jwtVerified = await request.jwtVerify<{user:PublicUserType}>();
        request.server.user=jwtVerified.user;
      } catch (err) {
        reply.send(err);
      }
    }
  );
done();
}
export default fp(jwtPlugin);