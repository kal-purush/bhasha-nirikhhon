import { SignJWT, jwtVerify, JWTPayload } from "jose";
import { TokenServiceError } from "./CustomErrors";

export interface ITokenService {
  generate(payload: object): Promise<string>;
  verify(token: string): Promise<JWTPayload>;
}

export default class TokenService {
  private secretKey: Uint8Array;

  constructor() {
    if (!process.env.JWT_SECRET) {
      throw new TokenServiceError(
        "JWT_SECRET environment variable is required.",
        "Check if the JWT_SECRET environment variable is defined",
        500,
        false,
        "Unreachable JWT_SECRET environment variable",
      );
    }
    this.secretKey = new TextEncoder().encode(process.env.JWT_SECRET);
  }

  async generate(payload: { userId: string }): Promise<string> {
    return new SignJWT(payload as JWTPayload)
      .setProtectedHeader({ alg: "HS256" })
      .setIssuedAt()
      .setExpirationTime("1h")
      .sign(this.secretKey);
  }

  async verify(token: string): Promise<JWTPayload> {
    try {
      const { payload } = await jwtVerify(token, this.secretKey, {
        algorithms: ["HS256"],
      });

      if (typeof payload.userId !== "string") {
        throw new TokenServiceError(
          "Invalid JWT payload type",
          "JWT Payload type must be string",
          500,
          false,
          "Invalid JWT payload type",
        );
      }

      return payload;
    } catch {
      throw new TokenServiceError("Invalid Token.", "Check the provided JWT Token", 500, false, "Invalid Token.");
    }
  }
}