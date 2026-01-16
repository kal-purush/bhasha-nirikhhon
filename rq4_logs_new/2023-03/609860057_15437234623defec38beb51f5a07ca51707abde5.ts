import { FastifyReply, FastifyRequest } from "fastify";
import { ColumnSchema } from "./column.schema";
import {
  createColumn,
  getColumns,
  getColumnById,
  updateColumn,
  deleteColumn,
} from "./column.service";

export const createColumnHandler = async (
  request: FastifyRequest<{
    Body: ColumnSchema;
  }>,
  reply: FastifyReply
) => {
  try {
    const body = request.body;
    const column = await createColumn(body);

    reply.code(200).send({ column });
  } catch (error) {
    console.log(error);
    reply.code(500).send({ error: "Internal server error" });
  }
};

export const getColumnsHandler = async (
  request: FastifyRequest,
  reply: FastifyReply
) => {
  try {
    const column = await getColumns();

    reply.code(200).send({ column });
  } catch (error) {
    console.log(error);
    reply.code(500).send({ error: "Internal server error" });
  }
};

export const getColumnByIdHandler = async (
  request: FastifyRequest<{
    Params: { id: string };
  }>,
  reply: FastifyReply
) => {
  try {
    const { id } = request.params;
    const column = await getColumnById(id);

    reply.code(200).send({ column });
  } catch (error) {
    console.log(error);
    reply.code(500).send({ error: "Internal server error" });
  }
};

export const updateColumnHandler = async (
  request: FastifyRequest<{
    Body: ColumnSchema;
    Params: { id: string };
  }>,
  reply: FastifyReply
) => {
  try {
    const { id } = request.params;
    const body = request.body;
    const column = await updateColumn(id, body);

    reply.code(200).send({ column });
  } catch (error) {
    console.log(error);
    reply.code(500).send({ error: "Internal server error" });
  }
};

export const deleteColumnHandler = async (
  request: FastifyRequest<{ Params: { id: string } }>,
  reply: FastifyReply
) => {
  try {
    const { id } = request.params;
    const column = await deleteColumn(id);

    reply.code(200).send({ column });
  } catch (error) {
    console.log(error);
    reply.code(500).send({ error: "Internal server error" });
  }
};