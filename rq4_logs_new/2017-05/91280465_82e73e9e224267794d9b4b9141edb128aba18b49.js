import Koa from 'koa';
import Router from 'koa-router';
import Chalk from 'chalk';

const koa = new Koa();
const router = Router();

//deprected
// app.get('/', function *(){
//   console.log('Express-style example');
//   this.body = "This is root page ('/')";
// });

router
    .get('/:name', async (ctx, next) =>{
        ctx.body = 'Eae ' + ctx.params['name'];
        
        console.log(ctx.params);
        console.log(ctx.request);
        console.log(ctx.request.query);
        
    })
    .post('/', async (ctx, next) =>{
        ctx.body = 'post';
    });

//middleware
koa.use(async (ctx, next) => {
  await next();
  console.log(Chalk.green(`${ctx.request.method} ${ctx.request.url} ${ctx.response.status}`));
});

// koa.use(async (ctx) => {
//     console.log('msg');
//     ctx.body = 'Hello world';
// });

koa.use(router.routes());
koa.use(router.allowedMethods());
koa.listen(3000, () => {
    console.log('This server has been ignite.');
});