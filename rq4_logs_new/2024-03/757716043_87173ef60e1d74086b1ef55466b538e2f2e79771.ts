import { NextFunction, Request, Response } from 'express'
import ApiError from '../util/errorHandling/ApiError'

const errorMiddleware = (
  err: ApiError,
  req: Request,
  res: Response,
  next: NextFunction
) => {
  const message = err?.message ?? 'Unknown Error'
  const status = err?.statusCode ?? 500

  res.status(status).json({ message })
}

export default errorMiddleware