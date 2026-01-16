
import express from 'express'
import cors from "cors"
import getBalance from "./getBalance"
const app = express()
const port = 3000
 
app.use(cors())


app.get('/', async (_, res) => {
  const balance = await getBalance()
  res.send(balance)
})

app.listen(port, () => {
  console.log(`Example app listening at http://localhost:${port}`)
})