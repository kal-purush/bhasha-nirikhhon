import { NextFunction, Request, Response } from 'express'

import { Logger } from '@utils/logger'

export abstract class BaseErrorHandler {
  protected abstract readonly logger: Logger

  log(err: Error, _req: Request, _res: Response, next: NextFunction) {
    this.logger.error(err.message)

    next(err)
  }

  abstract httpException(
    err: Error,
    req: Request,
    res: Response,
    next: NextFunction,
  ): void

  abstract error(
    err: Error,
    req: Request,
    res: Response,
    next: NextFunction,
  ): void
}