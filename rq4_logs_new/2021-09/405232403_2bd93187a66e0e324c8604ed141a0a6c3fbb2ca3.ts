// error handling middleware
// throw err from API functions/controllers are processed by this middleware
import { Context } from 'koa';

const errorHandler = async (
  ctx: Context,
  next: (err?: any) => Promise<any>,
) => {
  try {
    await next();
  } catch (err: any) {
    console.error(err);
    if (err.status >= 500) console.error('Error handler:', err);
    ctx.status = err.statusCode || err.status || 500;
    ctx.body = {
      status: 'failed',
      message: err.message || 'Internal server error',
    };
  }
};

export default errorHandler;