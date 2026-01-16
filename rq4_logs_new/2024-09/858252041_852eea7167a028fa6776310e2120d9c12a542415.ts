import { FastifyBaseLogger } from "fastify";
import { TokenInfo } from "../plugins/tokenVerify.js";
import { config } from "../config.js";

export const verifyToken = async (
  accessToken: string,
  traceLogger: FastifyBaseLogger,
) => {
  const tokenInfo = await fetch(
    `https://www.googleapis.com/oauth2/v3/tokeninfo?access_token=${accessToken}`,
  );

  const tokenJson = await tokenInfo.json();

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
  traceLogger.debug({ isAllowedUser, isNotExpired }, "verified claims");

  return isAllowedUser && isNotExpired;
};