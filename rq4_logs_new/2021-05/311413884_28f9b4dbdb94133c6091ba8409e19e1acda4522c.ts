import { NextFunction, Response, Request } from "express";
import { authController } from "../controllers/auth";
import HttpError from "../lib/exceptions/error-handler";

export async function checkAuth(
  req: Request,
  _res: Response,
  next: NextFunction
) {
  try {
    const bearerToken = req.headers.authorization;
    const token = bearerToken?.split(" ")[1];
    const user = await authController.checkAccessToken(token!);
    req.user = user;
    next();
  } catch (error) {
    next();
  }
}

export async function protectRoute(
  req: Request,
  _res: Response,
  next: NextFunction
) {
  if (!req.user) {
    next(new HttpError("Unauthorized", 401));
  } else {
    next();
  }
}