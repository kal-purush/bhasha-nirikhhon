import { FastifyInstance } from "fastify";
import { ZodTypeProvider } from "fastify-type-provider-zod";
import { z } from "zod";
import { CreateTask, Tasks, TasksQueryResult } from "./models.js";
import { getTracingHeader } from "../../lib/utils.js";

export const postTask = async (fastify: FastifyInstance) => {
  fastify.withTypeProvider<ZodTypeProvider>().route({
    method: "POST",
    url: "/task",
    schema: {
      body: CreateTask,
      response: {
        200: Tasks,
        400: z.object({ error: z.string() }),
        500: z.object({ error: z.unknown() }),
      },
    },
    handler: async (request, reply) => {
      const { tracingId, key } = getTracingHeader(request.headers);
      reply.header(key, tracingId);
      const tasksLogger = fastify.log.child({
        tracingId,
      });

      tasksLogger.info("Starting to handle postTask-request");

      try {
        const query = `
          INSERT INTO tasks (name, assignee)
          VALUES ($1, $2)
          RETURNING id, name, "createdAt", "dueDate", assignee, done;
        `;

        const taskResult = await fastify.pgQuery({
          query,
          model: TasksQueryResult,
          values: [request.body.name, request.body.assignee],
          traceLogger: tasksLogger,
        });
        if (taskResult.isOk) {
          return reply.code(200).send({ tasks: taskResult.data });
        }

        return reply
          .code(400)
          .send({ error: "Error querying data from database" });
      } catch (error) {
        tasksLogger.error({ error }, "Error handling request");
        reply.code(500).send({ error: JSON.stringify(error) });
      }
    },
  });
};