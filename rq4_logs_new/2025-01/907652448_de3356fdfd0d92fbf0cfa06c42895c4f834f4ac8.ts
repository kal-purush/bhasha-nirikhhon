import {
  CanActivate,
  ExecutionContext,
  Inject,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { Request } from 'express';

import { IJwtTokenProvider } from '../../../core/auth';
import { authConst } from '../../../core/lib';
import { JwtTokenProvider } from '../providers';

@Injectable()
export class AccessTokenGuard implements CanActivate {
  constructor(
    @Inject(JwtTokenProvider) private readonly jwtProvider: IJwtTokenProvider,
    private readonly reflector: Reflector,
  ) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const isPublic = this.reflector.getAllAndOverride<boolean>(
      authConst.PUBLIC_KEY,
      [context.getHandler(), context.getClass()],
    );

    if (isPublic) {
      return true;
    }

    const request = context.switchToHttp().getRequest() as Request;

    const token = request.headers.authorization;

    if (!token) {
      throw new UnauthorizedException();
    }

    try {
      const result = await this.jwtProvider.verify(token);

      if (!result.isValid) {
        return false;
      }
      request[authConst.USER_KEY] = result.payload;
    } catch (_) {
      return false;
    }

    return true;
  }
}