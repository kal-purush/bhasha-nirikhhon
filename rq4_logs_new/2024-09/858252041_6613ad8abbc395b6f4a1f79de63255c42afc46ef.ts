import { FastifyInstance } from "fastify";
import { ZodTypeProvider } from "fastify-type-provider-zod";
import { z } from "zod";
import { UsersQueryResult } from "./models.js";
import { getTracingHeader } from "../../lib/utils.js";

export const getUsers = async (fastify: FastifyInstance) => {
  fastify.withTypeProvider<ZodTypeProvider>().route({
    method: "GET",
    url: "/users",
    schema: {
      response: {
        200: UsersQueryResult,
        500: z.object({ error: z.unknown() }),
      },
    },
    handler: async (request, reply) => {
      const { tracingId, key } = getTracingHeader(request.headers);
      reply.header(key, tracingId);
      const usersLogger = fastify.log.child({
        tracingId,
      });
      usersLogger.info("Starting to handle getUsers-request");

      try {
        const query = "SELECT * FROM users";

        const usersResult = await fastify.pgQuery({
          query,
          model: UsersQueryResult,
          values: [],
          traceLogger: usersLogger,
        });
        if (usersResult.isOk) {
          return reply.code(200).send(usersResult.data);
        }

        usersLogger.error({ error: usersResult.data }, "Error parsing users");
        return reply
          .code(400)
          .send({ error: "Erorr querying data from database" });
      } catch (error) {
        usersLogger.error({ error }, "Error handling request");
        reply.code(500).send({ error: JSON.stringify(error) });
      }
    },
  });
};