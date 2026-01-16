import * as dotenv from 'dotenv'
import * as express from 'express'
import * as mongoose from 'mongoose'
import routes from './routes'
const { log } = console

dotenv.config()

// DB
const dbUri = process.env.DB_CONNECTION_URI
mongoose.connect(dbUri, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
  useFindAndModify: false,
  useCreateIndex: true,
})

// SERVER
const port = process.env.PORT
const app = express()
app.use(routes)
app.listen(port, () => {
  log(`Server started at ${port}`)
})