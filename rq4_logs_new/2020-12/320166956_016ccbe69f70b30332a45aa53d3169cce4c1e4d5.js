const express = require('express')
const cors = require('cors')
const knex = require('knex')({
  client: 'pg',
  connection: 'postgresql://postgres:secret@db/postgres'
})
const bodyParser = require('body-parser')

const app = express()

app.use(cors())
app.use(bodyParser.json())

app.use((req, res, next) => {
  console.log('received request at', req.path)
  next()
})

app.get('/api/things', async (req, res) => {
  const things = await knex.select().from('things');
  res.json(things)
})

app.post('/api/things', async (req, res) => {
  const name = req.body?.name
  if (!name) {
    res.status(422).json({
      message: 'Validation error "name" is required',
    })
    return
  }

  await knex('things').insert({name})

  res.json({})
})

app.listen(3000, () => console.log('started'))