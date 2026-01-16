import Koa from 'koa'

const createServer = () => {
  const app = new Koa()
  // 当前根目录
  const root = process.cwd()

  const ctx = {
    app,
    root
  }

  // 应用内置 / 配置而来的中间件

  return app
}

export default createServer