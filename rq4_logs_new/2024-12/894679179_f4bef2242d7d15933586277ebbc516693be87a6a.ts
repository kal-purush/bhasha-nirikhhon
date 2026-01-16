import { ZodSchema } from 'zod';
import { AppError, ValidationError } from '../utils/AppError';
import { Request } from 'express';
import { NextFunction, Response } from 'express-serve-static-core';
import { getErrorsFromIssues } from '../utils/zod.utils';

export function validateRequest<T>(schema: ZodSchema<T>) {
  return (req: Request, res: Response, next: NextFunction) => {
    const parseResult = schema.safeParse(req.body);

    if (!parseResult.success) {
      const errorObject = getErrorsFromIssues(parseResult.error.issues);
      const error = new ValidationError(errorObject, 'Validation failed');

      res.status(422).json({
        message: error.message,
        errorDetails: error.errorDetails,
      });
      return;
    }

    next();
  };
}