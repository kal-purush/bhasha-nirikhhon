import express, { json } from 'express'
import cors from 'cors'
import * as dotenv from 'dotenv'
import { PrismaClient } from '@prisma/client'
import { router } from './routes/router'

dotenv.config()

export const prisma = new PrismaClient()

const PORT = process.env.PORT || 3000

const server = express()

server.use(json())
server.use(cors())

server.use('/api/v1', router)

server.get('/', (_req, res) => {
  res.status(200).send('Iniciar la api en el endpoint: api/v1/entity')
})

server.listen(PORT, () => {
  console.log(`Listen on port ${PORT}`)
})