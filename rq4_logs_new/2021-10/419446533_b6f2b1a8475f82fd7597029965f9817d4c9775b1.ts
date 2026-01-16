import { NextFunction, Request, Response } from 'express';
import { verify, JwtPayload } from 'jsonwebtoken';

export function ensureAuthenticated(request: Request, response: Response, next: NextFunction) {
  const { authorization } = request.headers;

  if (!authorization) {
    return response.status(401).json({
      error: 'Unauthorized',
      message: 'Missing access token',
      code: 'token.missing',
    });
  }

  const [, accessToken] = authorization.trim().split(' ');

  try {
    const { sub } = verify(accessToken, process.env.JWT_SECRET) as JwtPayload;

    request.user_id = sub;

    return next();
  } catch (error) {
    return response.status(401).json({
      error: 'Unauthorized',
      message: 'The token provided is invalid or has expired',
      code: 'token.invalid',
    });
  }
}