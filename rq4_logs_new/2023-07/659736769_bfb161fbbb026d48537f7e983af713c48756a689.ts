import {
  HttpException,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';
import { PassportStrategy } from '@nestjs/passport';
import { Strategy } from 'passport-local';
import { UserAuth } from '../auth-user';

@Injectable()
export class LocalStrategy extends PassportStrategy(Strategy) {
  constructor(private userAuth: UserAuth) {
    super();
  }

  async validate(email: string, password: string) {
    const user = await this.userAuth.execute(email, password);

    if (!user) {
      throw new HttpException('Email ou senha incorreto!', 401);
    }

    return user;
  }
}