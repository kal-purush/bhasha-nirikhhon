import { FastifyReply, FastifyRequest } from "fastify";
import { BoardSchema } from "./board.schema";
import {
  createBoard,
  deleteBoard,
  getBoardById,
  getBoards,
  updateBoard,
} from "./board.service";

export const createBoardHandler = async (
  request: FastifyRequest<{ Body: BoardSchema }>,
  reply: FastifyReply
) => {
  try {
    const body = request.body;
    const board = await createBoard(body);

    reply.code(201).send({ board });
  } catch (error) {
    console.log(error);
    reply.code(500).send({ error: "Incorrect value was provided" });
  }
};

export const getBoardsHandler = async (
  request: FastifyRequest,
  reply: FastifyReply
) => {
  try {
    const board = await getBoards();

    reply.code(200).send({ board });
  } catch (error) {
    console.log(error);
    reply.code(500).send({ error: "Internal server error" });
  }
};

export const getBoardByIdHandler = async (
  request: FastifyRequest<{ Params: { id: string } }>,
  reply: FastifyReply
) => {
  try {
    const { id } = request.params;
    const board = await getBoardById(id);

    reply.code(200).send({ board });
  } catch (error) {
    console.log(error);
    reply.code(500).send({ error: "Internal server error" });
  }
};

export const updateBoardHandler = async (
  request: FastifyRequest<{ Body: BoardSchema; Params: { id: string } }>,
  reply: FastifyReply
) => {
  try {
    const { id } = request.params;
    const body = request.body;
    const board = await getBoardById(id);

    if (!board) {
      reply.code(404).send({ error: "Board not found" });
    }

    const updatedBoard = await updateBoard(id, body);

    reply.code(200).send({ board: updatedBoard });
  } catch (error) {
    console.log(error);
    reply.code(500).send({ error: "Internal server error" });
  }
};

export const deleteBoardHandler = async (
  request: FastifyRequest<{ Params: { id: string } }>,
  reply: FastifyReply
) => {
  try {
    const { id } = request.params;
    const board = await getBoardById(id);

    if (!board) {
      reply.code(404).send({ error: "Board not found" });
    }

    const deletedBoard = await deleteBoard(id);

    reply.code(200).send({ board: deletedBoard });
  } catch (error) {
    console.log(error);
    reply.code(500).send({ error: "Internal server error" });
  }
};