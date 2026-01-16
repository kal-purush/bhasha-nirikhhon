import { CallHandler, ExecutionContext, Injectable, NestInterceptor } from '@nestjs/common';
import { Observable } from 'rxjs';
import { tap } from 'rxjs/operators'
import { Request } from 'express';

interface RequestWrapper extends Request {
  customData: {
    valueOne: string
  }
}

@Injectable()
export class TasksInterceptor implements NestInterceptor {
  intercept(context: ExecutionContext, next: CallHandler): Observable<any> {   
    const now = Date.now();
    const req = context.switchToHttp().getRequest<RequestWrapper>();
    
    switch (req.method) {
      case 'GET':
        this.addNewCats();
        break;
      case 'POST':
        req.customData = {
          valueOne: 'example data'
        }
        break;
    }
   
    return next
      .handle()
      .pipe(
        tap(() => console.log(`After... ${Date.now() - now}ms`)),
      );
  }

  addNewCats(): void {
    console.log('add new cats');
  }
}