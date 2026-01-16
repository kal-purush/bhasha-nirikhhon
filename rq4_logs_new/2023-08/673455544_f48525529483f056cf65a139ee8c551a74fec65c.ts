import { Injectable, CanActivate, ExecutionContext } from '@nestjs/common';
import { Request } from 'express';
import { UnauthorizedException } from '@nestjs/common';
import { User } from '../entities/user.entity';

export class UserAdmPermissionException extends UnauthorizedException {
  constructor() {
    super('Only admin user is allowed to access this feature');
  }
}

@Injectable()
export class UserAdmPermissionGuard implements CanActivate {
  canActivate(context: ExecutionContext): boolean {
    const request = context.switchToHttp().getRequest<Request>();
    const user: User = request.user as User;
    const isAdm = user.isAdm;

    if (!isAdm) {
      throw new UserAdmPermissionException();
    }
    return true;
  }
}