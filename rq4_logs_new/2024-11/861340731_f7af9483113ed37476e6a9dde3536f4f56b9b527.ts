import { CanActivate, ExecutionContext, Injectable } from '@nestjs/common';
import { AuthGuard } from '@nestjs/passport';
import { Observable } from 'rxjs';
import * as process from 'process';

export const internalKey = process.env.INTERNAL_KEY;
@Injectable()
export class AdminGuard implements CanActivate {
  canActivate(
    context: ExecutionContext,
  ): boolean | Promise<boolean> | Observable<boolean> {
    const requestHeaders = context.switchToHttp().getRequest().headers;
    const isApiKey = requestHeaders['adminkey'] === internalKey;
    return isApiKey;
  }
}