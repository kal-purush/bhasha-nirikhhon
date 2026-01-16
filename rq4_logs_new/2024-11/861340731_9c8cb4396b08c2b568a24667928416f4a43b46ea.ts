import { Injectable, NestMiddleware } from '@nestjs/common';
import { AsyncLocalStorage } from 'async_hooks';
import { Request, Response } from 'express';
import { v4 as uuidV4 } from 'uuid';

export const ramdomUUId = () => uuidV4();
@Injectable()
export class TracingLoggerMiddleware implements NestMiddleware {
  constructor(private readonly als: AsyncLocalStorage<any>) {}

  use(req: Request, res: Response, next: (error?: any) => void): any {
    const tracingId: string = ramdomUUId();
    console.log(tracingId);
    const store = { tracingId };
    res.setHeader('trace-id', tracingId);
    this.als.run(store, () => next());
  }
}