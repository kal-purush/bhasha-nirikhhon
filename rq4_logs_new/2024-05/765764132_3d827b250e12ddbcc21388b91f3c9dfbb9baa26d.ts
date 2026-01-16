import { dbConnect } from './init'
import dotenv from 'dotenv'
import express, { type Express } from 'express'
import { createProxyMiddleware } from 'http-proxy-middleware'
import cors from 'cors'

dotenv.config()

const PORT = Number(process.env.SERVER_PORT) || 3001

const applyProxy = (app: Express) => {
    const {
        EXTERNAL_SERVER_URL,
        SERVER_URL = `http://localhost:${PORT}`,
        EXTERNAL_SERVER_PATH = '/ya',
    } = process.env
    const { hostname } = new URL(SERVER_URL)

    app.use(
        EXTERNAL_SERVER_PATH,
        createProxyMiddleware({
            target: EXTERNAL_SERVER_URL,
            changeOrigin: true,
            cookieDomainRewrite: hostname,
        })
    )
}

export const startApp = async () => {
    await dbConnect()
    const app = express()
    app.use(cors())

    applyProxy(app)

    app.listen(PORT, () => {
        console.log(`  ➜ 🎸 Server is listening on port: ${PORT}`)
    })
}