import { User } from 'src/user/entity/user.entity';

export interface LoginResult {
  token: string;
  user: User;
}