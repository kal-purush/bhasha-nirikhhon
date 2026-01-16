import { badRequest } from '@hapi/boom';
import { RequestHandler } from 'express';
import jwt from 'jsonwebtoken';
import env from '../config/env';

export const authorize: RequestHandler = async (req, res, next) => {
  const { accessToken } = req.cookies;

  try {
    if (!accessToken) throw new Error();

    const payload = jwt.verify(accessToken, env.JWT_SECRET);

    if (typeof payload === 'string' || typeof payload.userId !== 'string')
      throw new Error();

    res.locals.userId = Number(payload.userId);

    next();
  } catch {
    next(badRequest('인증 정보가 유효하지 않습니다.'));
  }

  next();
};