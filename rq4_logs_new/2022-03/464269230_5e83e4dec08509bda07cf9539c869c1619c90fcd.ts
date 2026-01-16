import { CanActivate, ExecutionContext, Injectable } from '@nestjs/common';
import { Reflector } from '@nestjs/core';

@Injectable()
export class RoleGuard implements CanActivate {

   constructor(private readonly _reflector: Reflector) { }
   
   canActivate(context: ExecutionContext): boolean {
      //obtiene los metodos que se pasaron en el decorador Roles()
      const roles: string[] = this._reflector.get<string[]>('roles', context.getHandler());

      if (!roles) {
         true;
      }

      const request = context.switchToHttp().getRequest();
      const { user } = request;
      const hasRole = () =>user.roles.some((role: string) => roles.includes(role));
      return user && user.roles && hasRole();
   }
}