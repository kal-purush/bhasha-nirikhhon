import fastify, { FastifyServerOptions } from "fastify";
import cors from "@fastify/cors";

import { apiRoutes } from "src/routes/v1";

export const startApp = (options?: FastifyServerOptions) => {
  const server = fastify({
    logger: {
      transport: {
        target: "pino-pretty",
      },
    },
    ...options,
  });

  server.register(cors, {});
  server.register(apiRoutes, { prefix: "/v1" });

  return server;
};