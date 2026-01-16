import { FastifyReply, FastifyRequest } from "fastify";
import { SubtaskSchema } from "./subtask.schema";
import {
  createSubtask,
  deleteSubtask,
  getSubtaskById,
  getSubtasks,
  updateSubtask,
} from "./subtask.service";

export const createSubtaskHandler = async (
  request: FastifyRequest<{ Body: SubtaskSchema }>,
  reply: FastifyReply
) => {
  try {
    const body = request.body;
    const subtask = await createSubtask(body);

    reply.code(200).send({ subtask });
  } catch (error) {
    console.log(error);
    reply.code(500).send({ error: "Internal server error" });
  }
};

export const getSubtasksHandler = async (
  request: FastifyRequest,
  reply: FastifyReply
) => {
  try {
    const subtask = await getSubtasks();

    reply.code(200).send({ subtask });
  } catch (error) {
    console.log(error);
    reply.code(500).send({ error: "Internal server error" });
  }
};

export const getSubtaskByIdHandler = async (
  request: FastifyRequest<{ Params: { id: string } }>,
  reply: FastifyReply
) => {
  try {
    const { id } = request.params;
    const subtask = await getSubtaskById(id);

    reply.code(200).send({ subtask });
  } catch (error) {
    console.log(error);
    reply.code(500).send({ error: "Internal server error" });
  }
};

export const updateSubtaskHandler = async (
  request: FastifyRequest<{ Body: SubtaskSchema; Params: { id: string } }>,
  reply: FastifyReply
) => {
  try {
    const { id } = request.params;
    const body = request.body;
    const subtask = await updateSubtask(id, body);

    reply.code(200).send({ subtask });
  } catch (error) {
    console.log(error);
    reply.code(500).send({ error: "Internal server error" });
  }
};

export const deleteSubtaskHandler = async (
  request: FastifyRequest<{ Params: { id: string } }>,
  reply: FastifyReply
) => {
  try {
    const { id } = request.params;
    const subtask = await deleteSubtask(id);

    reply.code(200).send({ subtask });
  } catch (error) {
    console.log(error);
    reply.code(500).send({ error: "Internal server error" });
  }
};