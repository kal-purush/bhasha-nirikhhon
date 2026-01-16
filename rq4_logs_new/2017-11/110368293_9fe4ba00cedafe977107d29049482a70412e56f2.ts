import * as express from 'express'
import * as next from 'next'

const DEFAULT_PORT = '3000'
// tslint:disable-next-line
const port = parseInt(process.env.PORT || DEFAULT_PORT, 10)
const dev = process.env.NODE_ENV !== 'production'
const app = next({ dev })
const handle = app.getRequestHandler()

app.prepare().then(() => {
  const server = express()

  server.get('*', (req, res) => {
    return handle(req, res)
  })

  server.listen(port, (err: Error) => {
    if (err) throw err
    console.log(`> Ready on http://localhost:${port}`)
  })
})