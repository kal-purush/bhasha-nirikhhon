import { config } from '../../../../config';
import jwt, { JwtPayload, Secret } from 'jsonwebtoken';
import { Request, Response, NextFunction } from 'express';

export const SECRET_KEY: Secret = config.jwtSecret;

export interface CustomRequest extends Request {
  user?: JwtPayload | string;
}

export const authToken = (
  req: Request,
  res: Response,
  next: NextFunction,
): void => {
  try {
    const token = req.header('Authorization')?.replace('Bearer ', '');
    if (!token) {
      res.status(401).json({ error: 'Access denied. No token provided.' });
      return;
    }

    const decoded = jwt.verify(token, SECRET_KEY) as JwtPayload;
    (req as CustomRequest).user = decoded;
    next();
  } catch (error) {
    res.status(400).json({ error: 'Invalid token.' });
  }
};