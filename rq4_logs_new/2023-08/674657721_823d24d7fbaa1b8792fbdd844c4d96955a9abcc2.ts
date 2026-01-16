import 'express-async-errors'
import express, { NextFunction, Request, Response } from 'express'
import { AppError } from '@/domain/errors/AppError'
import { DomainError } from '@/domain/errors/DomainError'

const app = express()

app.use(express.json())

app.use((err: Error, req: Request, res: Response, _: NextFunction) => {
  if (err instanceof AppError) {
    return res.status(err.statusCode).json({ message: err.message })
  }
  if (err instanceof DomainError) {
    return res.status(err.statusCode).json({ message: err.message })
  }
  return res
    .status(500)
    .json({ message: `Internal server error - ${err.message}` })
})

export { app }