import { RequestHandler } from 'express';
import HttpException from '../utils/HttpException';
import jwt from 'jsonwebtoken';
import { UserDto } from './../resources/users/interfaces/user.interface';
import { UserRepository } from './../resources/users/user.repository';

const userRepository = new UserRepository();

export const checkRole = (roles: string[]): RequestHandler => {
  return async (req, res, next) => {
    const headerToken = req.headers.authorization;
    const decodedToken = jwt.decode(headerToken.replace('Bearer ', ''), { complete: true });
    const { id } = decodedToken.payload;

    const findUser: UserDto = await userRepository.find(id);
    console.log('findUser: ', findUser);

    if (!roles.includes(findUser.role)) {
      return next(new HttpException(400, 'You are not manger'));
    }

    return next();
  };
};