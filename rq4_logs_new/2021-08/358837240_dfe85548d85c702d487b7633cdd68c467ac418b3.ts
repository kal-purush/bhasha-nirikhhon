// import { createHmac } from 'crypto';
import { NextFunction, Request, Response } from 'express';

export const validTokenNeeded = (req: Request, res: Response, next: NextFunction): Response | void => {
  const authorization = req.get('Authorization') as string;

  if (authorization.split(' ')[0] === 'Bearer') {
    const currentApiKey = authorization.split(' ')[1];
    if (isValidKey(currentApiKey)) {
      return next();
    } else {
      return res.status(403).send('Forbidden');
    }
  } else {
    return res.status(401).send('Unauthorized');
  }
};

export const apiKeyNeeded = (req: Request, res: Response, next: NextFunction): Response | void => {
  if (!req.get('Authorization')) {
    return res.status(401).send('Unauthorized');
  }

  return next();
};

const isValidKey = (currentApiKey: string): boolean => {
  return currentApiKey === process.env.API_KEY; // string used as API key
};