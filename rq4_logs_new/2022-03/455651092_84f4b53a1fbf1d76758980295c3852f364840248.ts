import { removeDeepData } from '@euk-labs/beltz';
import {
  CallHandler,
  ExecutionContext,
  Injectable,
  NestInterceptor,
} from '@nestjs/common';
import { map } from 'rxjs';

@Injectable()
export class SensitiveDataInterceptor implements NestInterceptor {
  sensitiveData: string[];

  constructor(...sensitiveData: string[]) {
    this.sensitiveData = sensitiveData;
  }

  intercept(context: ExecutionContext, next: CallHandler) {
    return next.handle().pipe(
      map((response) => {
        if (response.data) {
          const data = removeDeepData(response.data, this.sensitiveData);
          return { ...response, data };
        }

        return removeDeepData(response, this.sensitiveData);
      }),
    );
  }
}