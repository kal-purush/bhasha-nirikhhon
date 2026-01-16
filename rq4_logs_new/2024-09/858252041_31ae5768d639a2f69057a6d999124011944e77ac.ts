import fp from "fastify-plugin";
import { getTracingId, getToken } from "../lib/utils.js";
import { FastifyBaseLogger } from "fastify";
import { z } from "zod";
import { config } from "../config.js";

// Only protect routes that are out of the google-system
const protectedResources = ["/tasks", "/task"];

const TokenInfo = z.object({
  azp: z.string(),
  aud: z.string(),
  sub: z.string(),
  scope: z.string(),
  exp: z.string(),
  expires_in: z.string(),
  email: z.string(),
  email_verified: z.string(),
  access_type: z.literal("offline"),
});

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
      const accessToken = getToken(request.headers);
      const isValid = await verifyToken(accessToken, traceLogger);
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

const verifyToken = async (
  accessToken: string,
  traceLogger: FastifyBaseLogger,
) => {
  const tokenInfo = await fetch(
    `https://www.googleapis.com/oauth2/v3/tokeninfo?access_token=${accessToken}`,
  );

  const tokenJson = await tokenInfo.json();
  console.log("tokenJson", tokenJson);

  const parsed = TokenInfo.safeParse(tokenJson);
  if (parsed.error) {
    traceLogger.error(
      { error: parsed.error },
      "Error parsing tokenInfo response",
    );
    return false;
  }

  const isAllowedUser = config.allowedUsers.includes(parsed.data.email);
  const isNotExpired = Number(parsed.data.expires_in) > 0;

  return isAllowedUser && isNotExpired;
};