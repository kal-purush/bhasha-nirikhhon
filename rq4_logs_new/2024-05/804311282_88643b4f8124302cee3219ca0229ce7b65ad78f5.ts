import { NextFunction as NextFunc, Request, Response } from 'express';
import { Container } from 'typedi';
import { User } from '../entities';
import { AuthService } from '../services';
import { successResponse } from '../helpers';
import { OK, CREATED } from 'http-status';
import { IUserResponse } from '../interfaces';

export class AuthController {
  static async register(req: Request, res: Response, next: NextFunc) {
    try {
      const authServiceInstance = Container.get(AuthService);
      const { user, token } = await authServiceInstance.signUp(req.body as User);
      successResponse<IUserResponse>(res, CREATED, 'User Registered ✅', {
        user,
        token,
      });
    } catch (error) {
      return next(error);
    }
  }

  static async login(req: Request, res: Response, next: NextFunc) {
    try {
      const { email, password } = req.body;
      const authServiceInstance = Container.get(AuthService);
      const { user, token } = await authServiceInstance.login(email, password);
      successResponse<IUserResponse>(res, OK, 'User logged In ✅', {
        user,
        token,
      });
    } catch (error) {
      return next(error);
    }
  }

  static async currentUser(req: Request, res: Response, next: NextFunc) {
    try {
      const authServiceInstance = Container.get(AuthService);
      const user = await authServiceInstance.currentUser(req.user);
      successResponse<User>(res, OK, 'current user ✅', user);
    } catch (error) {
      return next(error);
    }
  }
}