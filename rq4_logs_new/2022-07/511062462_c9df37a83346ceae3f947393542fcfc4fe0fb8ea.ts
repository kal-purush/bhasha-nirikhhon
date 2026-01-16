import {
  CanActivate,
  ExecutionContext,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';
import { Observable } from 'rxjs';

@Injectable()
export class JwtAuthGuard implements CanActivate {
  constructor(private jwtService: JwtService) {}

  canActivate(
    context: ExecutionContext,
  ): boolean | Promise<boolean> | Observable<boolean> {
    const req = context.switchToHttp().getRequest();
    try {
      const header = req.headers.authorization;
      const tokenType = header.split(' ')[0];
      const token = header.split(' ')[1];

      if (tokenType !== 'Bearer' || !token) {
        throw new UnauthorizedException({ message: 'User is not logged in!' });
      }

      const user = this.jwtService.verify(token);
      req.user = user;
      return true;
    } catch (error) {
      throw new UnauthorizedException({ message: 'User is not logged in!' });
    }
  }
}