import Koa from 'koa';
import rTracer from 'cls-rtracer';
import loggers from 'koa-log4';
import createModel from '../../models/user';
import db from '../../models';
const api = new Koa();
const logger = loggers.getLogger("users");


// response
api.use(async ctx => {
  const requestId = rTracer.id();
  logger.debug(`requestId: ${requestId}`);
  const User = createModel(db.sequelize);
  // const User = db.models["User"];
  const users = await User.findAll();
  const response = {
    data: {
      response: users
    },
    status: 200,
    message: "Success"
  };
  ctx.body = response;
  logger.info(`Give response ${JSON.stringify(response)}`);
});

api.use(async ctx => {
  logger.info(JSON.stringify(ctx.URL))
  if (ctx.URL.pathname === '/api/error') {
    throw new Error('Give an error');
  }
});

export default api;