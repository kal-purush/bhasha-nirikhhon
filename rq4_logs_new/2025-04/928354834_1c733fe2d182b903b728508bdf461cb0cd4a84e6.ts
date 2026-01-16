import { Request, Response, NextFunction } from 'express';
import {
  AppError,
  DatabaseError,
  ValidationError,
  AuthenticationError,
  NotFoundError
} from '../utilities/errors';
import logger from '../utilities/logger';
import { Prisma } from '@prisma/client';

/**
 * Handle Prisma specific errors with detailed messages
 * @see https://www.prisma.io/docs/reference/api-reference/error-reference
 */
const handlePrismaError = (err: Prisma.PrismaClientKnownRequestError) => {
  switch (err.code) {
    case 'P2002':
      const field = (err.meta?.target as string[])?.join(', ');
      return new ValidationError(`Duplicate value for ${field}. Please use a unique value.`);
    case 'P2025':
      return new NotFoundError(err.meta?.modelName as string || 'Record');
    case 'P2003':
      return new ValidationError('Invalid relationship data provided.');
    case 'P2014':
      return new ValidationError('Invalid data provided. The operation would violate required relations.');
    default:
      return new DatabaseError(`Database error occurred: ${err.message}`);
  }
};

// Handle JWT errors
const handleJWTError = () => new AuthenticationError('Invalid token. Please log in again.');

const handleValidationError = (err: any) => {
  const errors = Object.values(err.errors).map((el: any) => el.message);
  const message = `Invalid input data. ${errors.join('. ')}`;
  return new ValidationError(message);

};

export const globalErrorHandler = (
  err: Error | AppError,
  req: Request,
  res: Response,
  next: NextFunction
) => {
  let error = err instanceof AppError ? err : new AppError(err.message, 500);
  if (!(error instanceof AppError)) {
    error = new AppError(err.message, 500);
  }

  // Log error with enhanced context
  logger.error('Error occurred', {
    errorType: error.constructor.name,
    errorCode: error.code,
    statusCode: error.statusCode,
    message: error.message,
    stack: process.env.NODE_ENV === 'development' ? err.stack : undefined,
    requestContext: {
      id: req.headers['x-request-id'],
      path: req.path,
      method: req.method,
      query: req.query,
      body: req.body,
      userId: req.user ? (req.user as any).id : undefined,
      ip: req.ip,
      userAgent: req.headers['user-agent']
    }
  });

  // Handle specific error types
  if (err instanceof Prisma.PrismaClientKnownRequestError) {
    error = handlePrismaError(err);
  } else if (err.name === 'JsonWebTokenError') {
    error = handleJWTError();
  } else if (err.name === 'ValidationError') {
    error = handleValidationError(err);
  }

  // Development vs Production error response
  const response: any = {
    status: error.status,
    code: error.code,
    message: process.env.NODE_ENV === 'production' && !error.isOperational
      ? 'Something went wrong!'
      : error.message,
    requestId: req.headers['x-request-id']
  };

  // Add additional context in development
  if (process.env.NODE_ENV === 'development') {
    response.error = error;
    response.stack = err.stack;
    if (err instanceof Prisma.PrismaClientKnownRequestError) {
      response.prismaError = {
        code: err.code,
        meta: err.meta
      };
    }
  }

  res.status(error.statusCode).json(response);
};