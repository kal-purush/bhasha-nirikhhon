import express from 'express'
import path from 'path'
import open from 'open'
import webpack from 'webpack'
import config from '../webpack.config.dev'

/* eslint-disable no-console */

const port = 3000;
const app = express()
const compiler = webpack(config)

app.use(require('webpack-dev-middleware')(compiler, {
  noInfo: true,
  publicPath: config.output.publicPath
}))

app.get('/', function (req, res) {
  res.sendFile(path.join(__dirname, '../src/index.html'))
})

app.get('/users', function (req, res) {
  // ! hard coding for simplicity. pretend its a real data base
  res.json([
    { "id": 1, "firstName": "Bec", "lastName": "Hunt", "email": "hunt@gmail.com" },
    { "id": 1, "firstName": "Alex", "lastName": "Sanra", "email": "alex@gmail.com" },
    { "id": 1, "firstName": "Jess", "lastName": "Beling", "email": "jess@gmail.com" }
  ])
})

app.listen(port, function (err) {
  if (err) {
    console.log(err)
  } else {
    open('http://localhost:' + port)
  }
})