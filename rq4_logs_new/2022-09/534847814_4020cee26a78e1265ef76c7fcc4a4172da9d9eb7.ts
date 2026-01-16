import { NextFunction, Request, Response } from "express";
import { RedisClientType } from "redis";
import redisClient from "../utils/redis";

export const redisUser = async (
  req: Request,
  res: Response,
  next: NextFunction
) => {
  req.redis = (await redisClient()) as RedisClientType;
  next();
};