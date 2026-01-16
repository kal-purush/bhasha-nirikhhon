import { Response, Request, NextFunction } from 'express';
import { Error } from 'mongoose';
import { INTERNAL_SERVER_ERROR_CODE } from './statusCodes';

interface ResponseError extends Error {
  statusCode?: number;
}

export default (err: ResponseError, req: Request, res: Response, next: NextFunction) => {
  const { statusCode = INTERNAL_SERVER_ERROR_CODE, message } = err;
  res
    .status(statusCode)
    .send({
      message: statusCode === INTERNAL_SERVER_ERROR_CODE
        ? 'На сервере произошла ошибка'
        : message,
    });
  next();
};