import { CustomError } from '../../errors';
import { UsersResponse, UserResponse } from './../entities';

export interface UserRepository {
  getAll(): Promise<UsersResponse[]>,
  getById(id: string): Promise<UserResponse | CustomError>
}