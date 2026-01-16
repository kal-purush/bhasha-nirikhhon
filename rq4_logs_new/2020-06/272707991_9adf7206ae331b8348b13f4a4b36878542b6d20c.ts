import { CanActivate, ExecutionContext, Injectable } from '@nestjs/common';
import { Observable } from 'rxjs';
import { Reflector } from '@nestjs/core'

@Injectable()
export class RolesGuard implements CanActivate {
  user = {
    id: 1,
    name: 'task', 
    role: 'admin'
  }
  constructor(
    private readonly reflector: Reflector
  ) {}
  canActivate(
    context: ExecutionContext,
  ): boolean | Promise<boolean> | Observable<boolean> {
    // valida los roles
    const roles = this.reflector.get('roles', context.getHandler());
    console.log(roles);
    if(roles.includes(this.user.role)) {
      return true;
    } else {
      return false
    }
  }
}