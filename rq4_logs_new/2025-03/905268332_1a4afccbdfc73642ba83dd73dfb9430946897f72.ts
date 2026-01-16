import { NextFunction, Request, Response } from 'express';
import { z, ZodError, ZodTypeAny } from 'zod';

export function validateData(schema: z.ZodObject<Record<string, ZodTypeAny>>) {
  return (req: Request, res: Response, next: NextFunction) => {
    try {
      schema.parse(req.body);
      next();
    } catch (error) {
      if (error instanceof ZodError) {
        const errorMessages = error.errors.map((issue) => ({
          message: `${issue.path.join('.')} is ${issue.message}`,
        }));
        res.status(400).json({ error: 'Invalid data', details: errorMessages });
      } else {
        next(error);
      }
    }
  };
}