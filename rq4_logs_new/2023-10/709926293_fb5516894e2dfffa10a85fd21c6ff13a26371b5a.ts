import express from "express"
// To access the .env variables
import {config} from"dotenv"
import morgan from "morgan" 
import appRouter from "./routes/index.js"
config()
const app=express()

app.use(express.json())
// For log information , we remove it in the production 
app.use(morgan('dev'))

app.use("/api/v1",appRouter)
export default app 