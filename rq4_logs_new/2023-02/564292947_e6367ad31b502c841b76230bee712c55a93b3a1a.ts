import { NextFunction, Request, Response } from 'express';
import { getAuthorizationToken } from '../utils/getAuthorizationToken';
import { firebaseAdmin } from '../utils/firebase-admin';
import * as UserService from '../routes/user/user.service';

export const authMiddleware = async (
  req: Request,
  res: Response,
  next: NextFunction
) => {
  try {
    const token = getAuthorizationToken(req.headers?.authorization ?? '');
    if (token) {
      const response = await firebaseAdmin.auth().verifyIdToken(token);
      const user = await UserService.getUserByFirebaseId({
        firebaseId: response.uid,
      });
      // @ts-ignore
      req.userId = user?.id;
    } else {
      return res.status(401).json('Unauthorized user');
    }

    next();
  } catch (error) {
    return res.status(401).json('Unauthorized user');
  }
};