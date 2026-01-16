const express = require('express');
const app = express();
const http = require('http');
const server = http.createServer(app);
const { Server } = require("socket.io");
const io = new Server(server);

const users = {};

app.get('/', (req, res) => {
  res.sendFile(__dirname + '/index.html');
});

io.on('connection', (socket) => {
    socket.on('set nickname', (nickname) => {
        socket.nickname = nickname;
        users[socket.id] = nickname;
        io.emit('chat message', `${socket.nickname || 'Anonymous'} has connected`);
        io.emit('user list', Object.values(users));
    });

    socket.on('chat message', (msg) => {
        const messageWithNickname = `${socket.nickname || 'Anonymous'}: ${msg}`;
        socket.broadcast.emit('chat message', messageWithNickname);
    });

    socket.on('typing', (nickname) => {
        socket.broadcast.emit('typing', nickname);
    });

    socket.on('stop typing', () => {
        socket.broadcast.emit('stop typing');
    });

    socket.on('private message', ({ target, message }) => {
        const targetSocket = [...io.sockets.sockets.values()]
            .find(s => s.nickname === target);
        if (targetSocket) {
            targetSocket.emit('chat message', `(Private) ${socket.nickname}: ${message}`);
            socket.emit('chat message', `(Private to ${target}): ${message}`);
        } else {
            socket.emit('chat message', `User ${target} is not online.`);
        }
    });
    

    // Broadcast when a user disconnects
    socket.on('disconnect', () => {
        io.emit('chat message', `${socket.nickname || 'Anonymous'} has disconnected`);
        delete users[socket.id];
        io.emit('user list', Object.values(users));
    });
});

server.listen(3000, () => {
  console.log('listening on *:3000');
});