import jwt from 'jsonwebtoken';
import bcrypt from 'bcrypt';

import { User } from '../entities/User';
import { config } from '../../config';
import { prisma } from '../utils/prisma';
import { UserService } from './UserService';
import { IAuthService } from '../interfaces/IUser/IAuthService';
import { passwordHash } from '../utils/passwordHash';
import { UserRepository } from '../repositories/UserRepository';
import {
  LoginEmailAndPasswordDto,
  LoginFacebookDto,
  LoginGoogleDto,
  RegisterUserDto,
} from '../dtos/UserDto';

export class AuthService implements IAuthService {
  private constructor(readonly repository: UserRepository) {}

  public static build(repository: UserRepository) {
    return new AuthService(repository);
  }

  public async register(
    name: string,
    email: string,
    password: string,
  ): Promise<RegisterUserDto> {
    const userExists = await this.repository.findByEmail(email);
    if (userExists) throw new Error('User already exists');

    const passwordEncrypted = passwordHash(password);

    const userObj = User.create(name, email, passwordEncrypted);
    const savedUser = await this.repository.save(userObj);

    const output: RegisterUserDto = {
      id: savedUser.id,
      name: savedUser.name,
      email: savedUser.email,
      password: savedUser.password,
    };
    return output;
  }

  public async loginWithEmailAndPassword(
    email: string,
    password: string,
  ): Promise<LoginEmailAndPasswordDto> {
    const repository = UserRepository.build(prisma);
    const service = UserService.build(repository);

    const user = await service.findByEmail(email);
    if (!user) throw new Error('User not found');

    const passwordMatch = await bcrypt.compare(password, user.password);
    if (!passwordMatch) throw new Error('Invalid password');

    const payload = {
      id: user.id,
      email: user.email,
    };
    const token = jwt.sign(payload, config.jwtSecret);
    return { token, id: user.id, email: user.email };
  }

  loginWithGoogle(email: string, token: string): Promise<LoginGoogleDto> {
    throw new Error('Method not implemented.');
  }
  loginWithFacebook(email: string, token: string): Promise<LoginFacebookDto> {
    throw new Error('Method not implemented.');
  }
}