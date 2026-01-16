import {
  Injectable,
  NestMiddleware,
  BadRequestException,
} from '@nestjs/common';
import { Request, Response, NextFunction } from 'express';

/**
 * Middleware for validating input data when user logs in.
 *
 * Pipes (global ValidationPipe) run after guards (AuthGuard) so validation is done in a middleware instead.
 */
@Injectable()
export class LoginValidationMiddleware implements NestMiddleware {
  use(req: Request, res: Response, next: NextFunction) {
    const { email, password } = req.body;

    if (!email || !password) {
      throw new BadRequestException('Email and password are required');
    }

    next();
  }
}