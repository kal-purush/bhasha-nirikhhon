import { FastifyBaseLogger } from "fastify";
import { ZodType } from "zod";
import fp from "fastify-plugin";
import pg from "pg";
import { newUid } from "../lib/utils.js";
import { config } from "../config.js";

type PgQueryParams<T> = {
  query: string;
  values: Array<unknown>;
  model: ZodType<T>;
  traceLogger?: FastifyBaseLogger;
};

declare module "fastify" {
  interface FastifyInstance {
    pgQuery: <T>(data: PgQueryParams<T>) => Promise<Result<T>>;
  }
}

type Result<T> =
  | {
      isOk: true;
      data: T;
    }
  | {
      isOk: false;
      data: unknown;
    };

export default fp(async (fastify, _opts) => {
  const pgLogger = fastify.log.child({
    tracingId: `postgres-${newUid()}`,
  });
  pgLogger.info("Starting to create a postgres-connection");

  const pgClient = new pg.Client(config.pg);
  await pgClient.connect();

  fastify.decorate(
    "pgQuery",
    async <T>({
      traceLogger,
      query,
      values,
      model,
    }: PgQueryParams<T>): Promise<Result<T>> => {
      traceLogger?.debug(query, "Starting to query from pg");
      const result = await pgClient.query(query, values);
      const parsed = model.safeParse(result.rows);
      if (parsed.success) {
        traceLogger?.debug("Got data from pg");
        return { isOk: true, data: parsed.data };
      }

      traceLogger?.error({ error: parsed.error }, "Error parsing data");
      return { isOk: false, data: [] };
    },
  );
});