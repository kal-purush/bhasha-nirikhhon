import { badRequest } from '@hapi/boom';
import { NextFunction, Request, Response } from 'express';
import formidable from 'formidable';

export default function multipartParser(
  req: Request,
  res: Response,
  next: NextFunction
) {
  try {
    const form = formidable();

    form.parse(req, (err, field, files) => {
      if (err) throw err;
      if (!files.image) throw badRequest('image 필드가 없습니다.');

      res.locals.image = files.image;

      next();
    });
  } catch (err) {
    next(err);
  }
}