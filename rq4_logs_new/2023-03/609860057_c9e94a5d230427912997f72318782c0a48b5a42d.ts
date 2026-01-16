import { FastifyReply, FastifyRequest } from "fastify";
import { CreateTaskSchema } from "./task.schema";
import createTask from "./task.service";

export const createTaskHandler = async (
  request: FastifyRequest<{
    Body: CreateTaskSchema;
  }>,
  reply: FastifyReply
) => {
  const body = request.body;

  try {
    const task = await createTask(body);

    reply.code(201).send({ task });
  } catch (error) {
    console.log(error);
    reply.code(500).send({ error: "Incorrect value was provided" });
  }
};