const express = require('express')
const path = require('path');

const app = express();
const http = require('http').createServer(app)

app.use(express.static(path.join(__dirname, 'public')))


const io = require('socket.io')(http)

var people = [];

io.on('connection', socket => {
  console.log('Connected ')


    socket.on('login', (data) => {
    people.push(data);
    console.log(people);
    io.emit('online-user', {online: people});
  })

  socket.on('sendMessage', msg => {
    console.log(msg)
    socket.broadcast.emit('sendToAll', msg)
  })

  socket.on('typing', (data) => {
    console.log("typing");
    socket.broadcast.emit('display', data)

  })

  socket.on('untype', () => {
    socket.broadcast.emit('untype')
  })

  socket.on('disconnect-user', () => {
    socket.broadcast.emit('disconnect-user')
  })

  
})

const PORT = process.env.PORT || 3001

http.listen(PORT, () => {
  console.log(`Server is running at port ${PORT}`)
})

