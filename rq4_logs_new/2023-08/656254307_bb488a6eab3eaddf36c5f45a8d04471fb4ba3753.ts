import { ExpressMiddlewareInterface, Middleware } from 'routing-controllers';

@Middleware({ type: 'after' })
export class ExitMiddleware implements ExpressMiddlewareInterface {
  use() {
    // api exit
  }
}