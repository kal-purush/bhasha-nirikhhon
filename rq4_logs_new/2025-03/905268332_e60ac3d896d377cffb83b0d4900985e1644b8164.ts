import User from '../entities/User';
import { FindUserDto, LoginUserDto, RegisterUserDto } from '../dtos/UserDto';

export interface IUserRepository {
  save(user: User): Promise<void>;
  find(id: string): Promise<User | null>;
  findByEmail(email: string): Promise<User | null>;
  update(id: string, password: string): Promise<void>;
  delete(id: string): Promise<void>;
}

export interface IUserService {
  register(
    name: string,
    email: string,
    password: string,
  ): Promise<RegisterUserDto>;
  loginWithEmailAndPassword(
    email: string,
    password: string,
  ): Promise<LoginUserDto | null>;
  find(id: string): Promise<FindUserDto>;
  update(id: string, password: string): Promise<void>;
  delete(id: string): Promise<void>;
}