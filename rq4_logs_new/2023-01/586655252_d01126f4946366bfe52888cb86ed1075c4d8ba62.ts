import { badRequest } from '@hapi/boom';
import { NextFunction, Request, Response } from 'express';
import env from '../config/env';
import AuthService from '../services/auth.service';

import { authenticateWithKakao } from '../validation/auth.validation';

export default class AuthController {
  authService: AuthService;

  constructor() {
    this.authService = new AuthService();
  }

  static getAuthenticationFromKakao(
    req: Request,
    res: Response,
    next: NextFunction
  ) {
    const { KAKAO_REST_API_KEY, KAKAO_REDIRECT_URI } = env;

    try {
      res.redirect(
        `https://kauth.kakao.com/oauth/authorize?client_id=${KAKAO_REST_API_KEY}&redirect_uri=${KAKAO_REDIRECT_URI}&response_type=code`
      );
    } catch (err) {
      next(err);
    }
  }

  async authenticateWithKakao(req: Request, res: Response, next: NextFunction) {
    try {
      const { code, error } =
        await authenticateWithKakao.input.query.validateAsync(req.query);

      if (error) throw badRequest(error);

      const accessToken = await this.authService.authenticateWithKakao(code);

      await authenticateWithKakao.output.cookie.validateAsync({ accessToken });

      res
        .cookie('accessToken', accessToken, { httpOnly: true, secure: true })
        .redirect('http://localhost:3000');
    } catch (err) {
      next(err);
    }
  }
}