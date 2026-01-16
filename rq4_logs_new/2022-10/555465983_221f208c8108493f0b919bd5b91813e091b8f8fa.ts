import { Injectable, CanActivate, ExecutionContext } from '@nestjs/common'
import { Reflector } from '@nestjs/core';
import jwt_decode from "jwt-decode";
import { UsersService } from 'src/users/users.service';

@Injectable()
export class RolesGuard implements CanActivate {
  
  constructor(
    private reflector: Reflector,
    private usersService: UsersService
    ) {}


  async canActivate(context: ExecutionContext): Promise<boolean> {
    const token = context.getArgs()[0].headers.authentication.split(' ')[1];
    const { username }:any = jwt_decode(context.getArgs()[0].headers.authentication.split(' ')[1]);
    const user  = await this.usersService.findOne(username);
    const roles  = this.reflector.get<string[]>('roles', context.getHandler());
    const total_roles = roles.filter(role => role === user.role);
    if(total_roles.length >=1){
      return true;
    }
    else{
      return false;
    }
  }
}