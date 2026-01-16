const express = require('express');
const app = express();
const http = require('http');
const { Server } = require("socket.io");
const cors = require('cors');

app.use(cors());

const PORT = 3001;

const server = http.createServer(app);

const io = new Server(server, {
    cors: {
        origin: '*',
        methods: ['GET', 'POST'],
    }
});

io.on('connection', (socket) => {
    console.log(`User Connected: ${socket.id}`);

    socket.on('join_channelToken', (data) => {
        console.log(`User Joined Token: ${data}`);
        socket.join(data);
    });

    socket.on('send_message', (data) => {
        socket.to(data.channelToken).emit('receive_message', data);
    });
});

server.listen(PORT, () => {
    console.log('SERVER RUNNING ON PORT 3001');
});