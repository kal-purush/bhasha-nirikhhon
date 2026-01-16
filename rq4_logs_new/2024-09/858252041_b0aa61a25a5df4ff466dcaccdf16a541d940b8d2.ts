import fp from "fastify-plugin";
import { z } from "zod";
import JwksClient from "jwks-rsa";
import Jwt from "jsonwebtoken";
import { IncomingHttpHeaders } from "http";
import { getTracingId } from "../lib/utils.js";
import { FastifyBaseLogger } from "fastify";

const JwtTokenModel = z.object({
  appid: z.string(),
  tid: z.string(),
  exp: z.number(),
});

const protectedResources = ["/tasks", "/task"];

export default fp((fastify, _opts, done) => {
  fastify.addHook("onRequest", async (request, reply) => {
    const isProtectedResource = protectedResources.includes(request.url);
    if (!isProtectedResource) {
      return done();
    }

    const tracingId = getTracingId(request.headers);
    const traceLogger = fastify.log.child({ tracingId });
    traceLogger.debug("Starting to verify accessToken");

    try {
      const isValid = await verifyToken(getToken(request.headers), traceLogger);
      if (!isValid) {
        traceLogger.error("Error verifying accessToken: not valid");
        reply.code(403).send({ error: "Unauthorized" });
      }
    } catch (error) {
      traceLogger.error(error, "Error verifying accessToken");
      reply.code(403).send({ error: "Unauthorized" });
    }
  });

  done();
});

const getToken = (headers: IncomingHttpHeaders) => {
  const authHeader = headers["authorization"];
  return String(authHeader?.split(" ")[1]);
};

const getSigningKey = async (token: string) => {
  const client = JwksClient({
    jwksUri: "https://www.googleapis.com/oauth2/v3/certs",
  });
  const decoded = Jwt.decode(token, { complete: true });
  const signingKey = await client.getSigningKey(decoded?.header.kid);

  return signingKey;
};

export const verifyToken = async (
  token: string,
  traceLogger: FastifyBaseLogger,
) => {
  const key = await getSigningKey(token);
  const publickKey = key.getPublicKey();
  traceLogger.debug("Got public-key");

  const verified = Jwt.verify(token, publickKey);
  const decoded = JwtTokenModel.parse(verified);
  traceLogger.debug({ decoded }, "Verified signature and decoded token");

  // Verify claims

  // the EXP property on a token is defined in seconds, whereas Date.now() returns milliseconds
  const isNotExpired = Number(decoded.exp) * 1000 > Date.now();
  const isTokenValid = isNotExpired;

  traceLogger.debug(`Verified claims result: ${isTokenValid}`);
  return isTokenValid;
};