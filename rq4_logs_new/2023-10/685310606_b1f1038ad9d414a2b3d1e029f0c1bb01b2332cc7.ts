import { NextFunction, Request, Response } from 'express';
import HttpException from '../utils/httpException.util';
import mapError from '../utils/mapError.util';
import { IUserCreate } from '../Interfaces/IUser';
import registerNewUserSchema from './schemas/register.schema';

const validateRegisterNewUserMiddleware = (req: Request, _res: Response, next: NextFunction) => {
  const { email, password, userName }: IUserCreate = req.body;
  const { error } = registerNewUserSchema.validate({ email, password, userName });

  const isNewUserDataInvalid = error;
  if (isNewUserDataInvalid) {
    const { type, message } = error.details[0];
    const errorStatusCode = mapError(type);

    throw new HttpException(errorStatusCode, message);
  }

  next();
};

export default validateRegisterNewUserMiddleware;