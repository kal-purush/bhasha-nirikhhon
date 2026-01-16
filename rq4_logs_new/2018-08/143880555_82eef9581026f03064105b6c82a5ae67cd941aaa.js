const Koa = require('koa');
const app = new Koa();

app.use(async (ctx) => {
  ctx.body = 'Alexey';
});

app.listen(3500);