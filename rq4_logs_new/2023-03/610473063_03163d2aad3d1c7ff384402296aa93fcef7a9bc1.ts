import { Request, Response, NextFunction } from 'express';
import jwt from 'jsonwebtoken';
import dotenv from 'dotenv';
import { errorResponse } from '../Helpers/response';

dotenv.config();
const secrect = process.env.SECRET_KEY;

interface ITokenUser {
  id: string;
  iat: number;
  exp: number;
}

// eslint-disable-next-line import/prefer-default-export
export const tokenValidation = (
  req: Request,
  res: Response,
  next: NextFunction
) => {
  try {
    if (secrect === undefined)
      throw new Error('500-Error con la bases de datos');

    const token = req.header('auth-token');
    if (!token) throw new Error('400-Acceso denegado');

    const payload = jwt.verify(token, secrect) as ITokenUser;

    req.userId = payload.id;
    next();
  } catch (error: any) {
    if (typeof error === 'object' && error !== null && 'message' in error) {
      const errorMessage = (error as Error).message;
      errorResponse(req, res, errorMessage.split('-'));
    }
  }
};