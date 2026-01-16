import { UsersRepository } from '../typeorm/repositories/UsersRepository';
import { getCustomRepository } from 'typeorm';
import { User } from '../typeorm/entities/User';
import AppError from '@shared/errors/AppError';

export class ShowUsersService {
  public async execute(
    type: string,
    operator: string,
    value: string,
  ): Promise<User | undefined> {
    const usersRepository = getCustomRepository(UsersRepository);
    const user = await usersRepository.findCustom(type, operator, value);
    if (!user) throw new AppError('User not found');
    return user;
  }
}