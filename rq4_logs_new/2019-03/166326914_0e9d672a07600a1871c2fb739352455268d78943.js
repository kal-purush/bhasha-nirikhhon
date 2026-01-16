const mKoa = require('./manualKoa/mKoa/application')

const interceptor = require('./manualKoa/m-interceptor-mid')
const logger = require('./manualKoa/m-logger-mid')
const static = require('./manualKoa/static')

const app = new mKoa()
const staticMid = static()
// 以下均是中间件

app.use(interceptor)
app.use(logger)
app.use(staticMid)

app.use(async (ctx, next) => {
  ctx.body = '1'
  await next()
  ctx.body += '2'
})


app.use(async (ctx, next) => {
  ctx.body += '3'
  await next()
  ctx.body += '4'
})

app.use(async (ctx, next) => {
  ctx.body += '5'
  await new Promise(resolve => {
    setTimeout(() => {
      resolve()
    }, 2000)
  })
  await next()
  ctx.body += '6'
})

app.use(async (ctx, next) => {
  ctx.body += '7'
})


// hostname:0.0.0.0 表示IPV4的地址
app.listen(3001, '0.0.0.0')