import { type NextFunction, type Response, type Request } from "express";
import CustomError from "../CustomError";

export const notFoundError = (
  req: Request,
  res: Response,
  next: NextFunction
) => {
  const error = new CustomError(
    "Resource not found",
    404,
    "Endpoint not found"
  );
  next(error);
};

const generalError = (
  error: CustomError,
  req: Request,
  res: Response,
  next: NextFunction
) => {
  res
    .status(error.statusCode || 500)
    .json({ error: error.publicMessage || "Custom message" });
};