import { Request, Response } from 'express';
import { ApiError, NotFoundError, ServerError } from '../utils/errors';
import logger from '../utils/logger';

export const errorHandler = (
  error: NotFoundError | ServerError | Error,
  req: Request,
  res: Response,
  next: Function
): void => {
  // Convert any error to ApiError format
  const apiError = error instanceof ApiError 
    ? error 
    : ApiError.fromError(error);

  // Send error response
  res.status(apiError.statusCode).json({ 
    error: apiError.message,
  });
};