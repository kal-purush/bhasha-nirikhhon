import express from 'express'
import next from 'next'

// Router
import authRouter from './auth'

const port: number = Number(process.env.PORT) || 3000
const dev = process.env.NODE_ENV !== 'production'
const app = next({
  dev,
})
const handle = app.getRequestHandler()

app.prepare().then(() => {
  const server = express()

  server.use('/api/v1', authRouter)

  server.all('*', (req, res) => {
    return handle(req, res)
  })

  server.listen(port, err => {
    if (err) throw err
    console.log(`> Ciel Music ready on port ${port}`)
  })
})