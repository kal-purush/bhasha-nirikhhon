import { RequestHandler } from 'express';
import CognitoUserPool from '../../services/cognito';
import logger from '../../util/logger';

export type UserSummary = {
  id: string,
  username: string
}

const FindUserHandler : RequestHandler = async (req, res, next) => {
  const TAG = FindUserHandler.name;
  const { username } = req.query;
  if (username === undefined) {
    return next(new Error('The "username" query parameter is required'));
  }
  try {
    const user = await CognitoUserPool.GetByUsername(username as string);
    return res.status(200).json([user]);
  } catch (error) {
    logger.error(TAG, error);
    return res.status(200).json([]);
  }
};

export default FindUserHandler;