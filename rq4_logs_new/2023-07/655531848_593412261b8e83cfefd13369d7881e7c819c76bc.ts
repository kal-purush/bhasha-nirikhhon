import { NextFunction, Request, RequestHandler, Response } from 'express'

export const conditionalMiddleware =
  (middleware: RequestHandler, avoidablePaths: string | string[]) =>
  async (req: Request, res: Response, next: NextFunction) => {
    const shouldAvoidPath = (requestPath: string) => {
      if (Array.isArray(avoidablePaths))
        return avoidablePaths.some(pathToAvoid =>
          requestPath.includes(pathToAvoid),
        )

      if (typeof avoidablePaths === 'string')
        return requestPath.includes(avoidablePaths)

      return false
    }

    if (shouldAvoidPath(req.path)) return next()

    await Promise.resolve(middleware(req, res, next))
  }