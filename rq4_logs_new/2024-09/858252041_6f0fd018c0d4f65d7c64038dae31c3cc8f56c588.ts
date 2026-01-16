import { FastifyInstance } from "fastify";
import { TasksQueryResult } from "./models.js";

export const tasks = async (fastify: FastifyInstance) => {
  // connection-handler
  fastify.io.on("connection", async (socket) => {
    const socketLogger = fastify.log.child({
      tracingId: `user-${socket.handshake.auth.userId}-socket-connection`,
    });

    // get all tasks
    const query = `SELECT * from tasks;`;
    const tasksResult = await fastify.pgQuery({
      query,
      model: TasksQueryResult,
      values: [],
      traceLogger: socketLogger,
    });
    if (!tasksResult.isOk) {
      socketLogger.error({ error: tasksResult.data }, "Error querying tasks");
    } else {
      socket.emit("tasks:get", tasksResult.data);
      socketLogger.info("Tasks emitted");
    }

    // update a task
    socket.on("task:update", async (task) => {
      socketLogger.info("Starting to put task-row");
      const query = `
        UPDATE tasks
        SET name = $1, assigneeId = $2, "createdAt" = $3, "dueDate" = $4, comment = $5, done = $6
        WHERE id = $7
        RETURNING id, name, assigneeId, "createdAt", "dueDate", done;
      `;

      const { name, assigneeId, createdAt, dueDate, comment, done, id } = task;
      const taskResult = await fastify.pgQuery({
        query,
        model: TasksQueryResult,
        values: [name, assigneeId, createdAt, dueDate, comment, done, id],
        traceLogger: socketLogger,
      });
      if (!taskResult.isOk) {
        socketLogger.error({ error: tasksResult.data }, "Error updating task");
      } else {
        socket.emit("task:updated", taskResult.data[0]);
        socketLogger.info("Task updated and emitted");
      }
    });
  });
};

export default tasks;