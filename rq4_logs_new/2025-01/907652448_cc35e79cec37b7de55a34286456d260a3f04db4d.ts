import { Injectable, UnauthorizedException } from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';

import {
  IJwtTokenProvider,
  JwtPayload,
  JwtVerifyResult,
} from '../../../core/auth';

@Injectable()
export class JwtTokenProvider implements IJwtTokenProvider {
  constructor(private readonly jwtService: JwtService) {}

  public async sign(payload: Pick<JwtPayload, 'userId'>): Promise<string> {
    return this.jwtService.sign(payload);
  }
  public async verify(token: string): Promise<JwtVerifyResult> {
    try {
      const payload = await this.jwtService.verifyAsync<JwtPayload>(token);

      const isValid = payload.exp > payload.iat;

      return { payload, isValid, error: isValid ? null : 'expired' };
    } catch (_) {
      return { payload: null, isValid: false, error: 'invalid token' };
    }
  }
  public async decode(token: string): Promise<JwtPayload> {
    try {
      return this.jwtService.decode<JwtPayload>(token);
    } catch (_) {
      throw new UnauthorizedException();
    }
  }
}