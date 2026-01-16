import { NextFunction, Request, Response } from 'express';
import { Repository, getRepository } from 'typeorm';
import EntidadeAdmin from '@core/entities/administrador';
import ForbiddenError from '@core/errors/forbidden';
import errorHandler from '@app/middlewares/error-handler';

export default async function admin(req: Request, res: Response, next: NextFunction): Promise<void> {
  const repositoryAdmin: Repository<EntidadeAdmin> = getRepository(EntidadeAdmin);

  try {
    const admin = await repositoryAdmin.findOne({ where: { id: req.session.userID, deletedAt: null } });

    if (!admin) { throw new ForbiddenError(); }

    return next();
  }
  catch (err) {
    return errorHandler(err, req, res);
  }
}