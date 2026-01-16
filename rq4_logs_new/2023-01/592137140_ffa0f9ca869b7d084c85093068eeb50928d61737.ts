import { Router } from 'express';
import piuRouter from './piu.routes';
import userRouter from './user.routes';

const routes = Router();

routes.use(piuRouter);
routes.use(userRouter);

export default routes;