import { ErrorMessageEnum } from '@/enums/error-messages.enum';
import {
  Injectable,
  CanActivate,
  ExecutionContext,
  HttpException,
} from '@nestjs/common';

@Injectable()
export class AccessTokenGuard implements CanActivate {
  canActivate(context: ExecutionContext): boolean {
    const headers = context.getArgs()[2].req.headers;

    const accessToken = headers['access-token'];

    if (!accessToken) {
      throw new HttpException(ErrorMessageEnum.INVALID_ACCESS_TOKEN, 500);
    }

    return true;
  }
}