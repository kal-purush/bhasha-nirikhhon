import Router from 'koa-router';
import { render, handle, defaultReturn } from 'nextjs-koa-middleware';

const nextRouter = new Router();

nextRouter.use(defaultReturn());
nextRouter.get('/package/:name', render('Package'));
nextRouter.all('*', handle());

export default nextRouter;