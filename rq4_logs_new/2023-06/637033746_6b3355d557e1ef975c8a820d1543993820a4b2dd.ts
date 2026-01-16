import { NextFunction, Request, Response } from 'express';
import jwt from 'jsonwebtoken';
import { ObjectId } from 'mongoose';
import ApiError from '../types/errors';
import { HttpStatus, StatusMessages } from '../utils/constants';
import config from '../config';

interface IPayload {
  readonly _id: string | ObjectId;
}

interface IRequestWithUser extends Request {
  user?: IPayload;
}

export default (req: IRequestWithUser, res: Response, next: NextFunction) => {
  const { authorization } = req.headers;

  if (!authorization || !authorization.startsWith('Bearer ')) {
    throw new ApiError(HttpStatus.BAD_LOGIN, StatusMessages[401].Login);
  }

  const token = authorization.replace('Bearer ', '');
  let payload;

  try {
    payload = jwt.verify(token, config.JWT_SECRET);
  } catch (e) {
    const error = new ApiError(HttpStatus.BAD_LOGIN, StatusMessages[401].Login);
    next(error);
  }
  req.user = payload as IPayload;
  return next();
};