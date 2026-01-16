const { Socket } = require('dgram')
const express = require('express')
const app = express()
const http = require('http')
const server = http.createServer(app)
const {Server} = require('socket.io')
const io = new Server(server)

const port = 3000

app.set('view engine', 'pug')
app.get('/', (req, res) => {
  res.render('index', {title: "Forum Diskusi"})
})

io.on("connection", (socket) => {
    socket.on("message", (data) =>
    console.log("data =>", (data)))
    socket.broadcast.emit("message", data)
})

server.listen(port, () => {
  console.log(`Example app listening on port ${port}`)
})