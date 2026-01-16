// INTERFACE
import { UserSchemaInterface } from "../utilities/interfaces";
import { Request, Response, NextFunction, RequestHandler } from "express";
// JWT
import { attachJwtToCookie, verifyToken } from "../utilities/token";
// HTTP CODES
import { StatusCodes } from "http-status-codes";
// ERRORS
import { UnauthorizedError } from "../errors";
// MODELS
import { Token } from "../models";
// MONGOOSE
import { Types } from "mongoose";

declare global {
  namespace Express {
    interface Request {
      user?: UserSchemaInterface & { _id: Types.ObjectId };
    }
  }
}
const authUser: RequestHandler = async (req, res, next) => {
  try {
    const {
      access_token,
      refresh_token,
    }: { access_token: string; refresh_token: string } = req.signedCookies;

    if (access_token) {
      console.log(access_token);
      console.log(typeof access_token);

      const isVerified = verifyToken(access_token);
      req.user = isVerified.user;
      next();
      return;
    }
    const isVerified = verifyToken(refresh_token);

    const existingToken = await Token.findOne({
      user: isVerified.user.userId,
      refreshToken: refresh_token,
    });

    const ip = req.ip;
    const userAgent = req.headers["user-agent"];

    if (
      !existingToken ||
      !existingToken.isValid ||
      isVerified.ip !== ip ||
      isVerified.userAgent !== userAgent
    )
      throw new UnauthorizedError("access denied");

    attachJwtToCookie({
      res,
      user: isVerified.user,
      refreshToken: refresh_token,
      ip,
      userAgent,
    });
    req.user = isVerified.user;
    next();
  } catch (error) {
    throw new UnauthorizedError("access denied");
  }
};

export { authUser };