import { response } from 'express';
import { UserDto } from '../dto/user.dto';
import { UsersRepository } from '../typeorm/repositories/UsersRepository';
import { hash } from 'bcryptjs';
import { getCustomRepository } from 'typeorm';

export class CreateUsersService {
  public async execute({
    name,
    email,
    password,
    confirmPassword,
    telephone,
  }: UserDto) {
    const usersRepository = getCustomRepository(UsersRepository);
    const emailExist = await usersRepository.findByEmail(email);
    if (emailExist) throw new Error(`Email: ${email} already exist`);
    if (password !== confirmPassword)
      throw new Error(`error confirming password `);
    const hashPassword = await hash(password, 8);
    const user = await usersRepository.save({
      name,
      email,
      password: hashPassword,
      telephone,
    });
    return user;
  }
}