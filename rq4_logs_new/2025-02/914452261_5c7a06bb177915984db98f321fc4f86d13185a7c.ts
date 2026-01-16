import { NextFunction, Request, Response } from 'express';

export class InvalidInputError extends Error {}
export class NotFoundError extends Error {}
export class ConflictError extends Error {}

export const errorHandler = (
  error: unknown,
  req: Request,
  res: Response,
  next: NextFunction,
): void => {
  if (error instanceof InvalidInputError) {
    res.status(400).json({ error: error.message });
  }

  if (error instanceof NotFoundError) {
    res.status(404).json({ error: error.message });
  }

  if (error instanceof ConflictError) {
    res.status(409).json({ error: error.message });
  }

  if (error instanceof Error) {
    res
      .status(500)
      .json({ error: error.message || 'An internal server error occurred' });
  }

  next();
};