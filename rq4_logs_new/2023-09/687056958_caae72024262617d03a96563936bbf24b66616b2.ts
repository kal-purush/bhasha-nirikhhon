import { Request, Response, NextFunction } from 'express';
import { decodeToken } from '../validators/jwt/decoteToken'

const authUser = (req: Request, res: Response, next: NextFunction) => {
  const { authorization } = req.headers;
  const { id } = req.params
  if(!authorization) {
    next({status: 400, message: 'You need to be logged in.'});

  }
  const payload = decodeToken(authorization as string)
  if (payload.id !== parseInt(id)) {
    next({status: 400, message: 'Invalid token.'})
  }
  res.locals.user = payload
  next()

}

export default { authUser}