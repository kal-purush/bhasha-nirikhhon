import express, { NextFunction, Request, Response } from 'express'
import cookieParser from 'cookie-parser'
import fileUpload from 'express-fileupload'

import { ENVIROMENT, PATH } from './config'

import tryCatch from './utils/tryCatch'

import {
  authMiddleware
} from './middleware/auth.middleware'

import {
  routerCategory,
  routerLogin,
  routerProduct,
  routerSearch,
  routerUpload,
  routerUser
} from './routes'

class App {

  app: any
  constructor() {
    this.app = express()

    this.middleware()

    this.app.use(this.logMiddleware)

    this.routes()
  }

  middleware() {
    this.app.use(express.json())
    this.app.use(express.static('public'));
    this.app.use(cookieParser())
    this.app.use(fileUpload(
      {
        useTempFiles: true,
        tempFileDir: '/tmp/',
        createParentPath: true
      }
    ));
  }

  logMiddleware(req: Request, res: Response, next: NextFunction) {

    console.log(req.headers['user-agent'])
    console.log(req.url)
    console.log(req.method)
    next()
  }

  routes() {

    this.app.use('/', routerLogin)

    this.app.use(tryCatch(authMiddleware))

    this.app.use('/', routerUser)
    this.app.use('/', routerCategory)
    this.app.use('/', routerProduct)
    this.app.use('/', routerSearch)
    this.app.use('/', routerUpload)

    this.app.use(this.errorHandler)
  }

  errorHandler(err: any, req: Request, res: Response, next: NextFunction) {

    console.log(err)

    const error = err.message || err
    return res.status(500).json({ error })
  }

  start() {
    this.app.listen(ENVIROMENT.PORT, () => {
      console.log(`Server on port ${ENVIROMENT.PORT}`)
    })
  }

}

export default App