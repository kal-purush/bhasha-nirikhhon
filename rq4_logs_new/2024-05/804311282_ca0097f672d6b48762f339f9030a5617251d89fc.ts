import jwt from 'jsonwebtoken';
import { Repository } from 'typeorm';
import { Socket } from 'socket.io';
import { User } from '../entities';
import { config } from '../config';
import { UniversalRepository } from '../repositories';
import { dataSource } from '../database';
import { DecodedToken } from '../interfaces';

type NextFunction = (error?: any) => void;

// JWT middleware for socket authentication
const verifySocketAuth = async (socket: Socket, next: NextFunction) => {
  const authHeader =
    socket.handshake.auth.token || socket.handshake.headers['authorization'];

  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return next(new Error('Authentication - Invalid headers'));
  }

  const token: string = authHeader.split(' ')[1];

  try {
    if (!token) {
      return next(new Error('Invalid Token supplied'));
    }

    const decoded = jwt.verify(token, config.app.jwtSecret) as DecodedToken;

    if (!decoded) {
      return next(new Error('invalid auth credentials'));
    }

    const userRepo: Repository<User> = dataSource.getRepository(User);

    const user = await new UniversalRepository<User>(userRepo).findOne({
      where: { id: decoded.id },
    });

    if (!user) {
      return next(new Error('Invalid Email/Password'));
    }

    (socket as any).user = user.id;

    return next();
  } catch (error) {
    next(new Error('Invalid auth token'));
  }
};

export { verifySocketAuth };