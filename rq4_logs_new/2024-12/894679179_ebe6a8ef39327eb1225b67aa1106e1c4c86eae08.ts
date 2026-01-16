import { NextFunction, Request, Response } from 'express';
import { AppError } from '../utils/AppError';
import { verifyToken } from '../utils/jwt.utils';
import { CustomRequest } from '../types';

export const authenticateToken = (
  req: Request,
  res: Response,
  next: NextFunction
) => {
  const error = new AppError(
    'AppError',
    'Token is missing',
    401,
    'authenticateToken'
  );

  const token = req.cookies.token;
  if (!token) {
    throw error;
  }

  try {
    const userId = verifyToken(token);
    (req as CustomRequest).user = userId;
    next();
  } catch (err) {
    error.message = 'Invalid token';
    next(error);
  }
};