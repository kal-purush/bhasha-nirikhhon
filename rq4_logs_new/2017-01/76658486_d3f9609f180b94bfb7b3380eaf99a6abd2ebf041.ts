import { Request, Response, NextFunction } from 'express-serve-static-core';
import { getVersionInfo } from '../helpers/manifest';
import { extend } from '../helpers/util';

export function versionHandler(req: Request, res: Response, next: NextFunction): void {
  getVersionInfo(req.query.short != undefined ? 'short' : 'long')
    .then(content => {
      res.json(content);
      next();
    }).catch(reason => {
      console.error(reason);
      next(extend(new Error('Error while receiving version information'), {status: 500}));
  });
}