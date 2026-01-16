import Fastify from "fastify";
import { config } from "./src/config.js";
import {
  serializerCompiler,
  validatorCompiler,
} from "fastify-type-provider-zod";
import cors from "@fastify/cors";
import traceLogger from "./src/plugins/traceLogger.js";
import swagger from "./src/plugins/swagger.js";
import fastifyStatic from "@fastify/static";
import { getHealth } from "./src/routes/health/get.js";
import { toWwwHtmlDir } from "./src/lib/utils.js";
import { getUser } from "./src/routes/user/getUser.js";

const fastify = Fastify({
  logger: {
    level: config.logLevel,
  },
  disableRequestLogging: true, // we use our own tracelogger!
});

fastify.setValidatorCompiler(validatorCompiler);
fastify.setSerializerCompiler(serializerCompiler);

// Plugins
await fastify.register(cors, {
  origin: ["http://localhost:3001"],
});
fastify.register(traceLogger);
fastify.register(swagger);

// Routes
fastify.register(fastifyStatic, {
  root: toWwwHtmlDir(import.meta.url),
});
fastify.register(getUser);
fastify.register(getHealth);

const start = async () => {
  try {
    await fastify.listen({ port: config.port, host: "0.0.0.0" });
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

start();