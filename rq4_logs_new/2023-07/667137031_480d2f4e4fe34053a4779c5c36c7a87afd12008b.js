
'use strict'
require("dotenv").config();
const PORT = process.env.PORT || 3000;
const io = require('socket.io')(PORT);


require("dotenv").config();
const airlineSocket = io.of('/airline');



io.on('connection', (socket) => {
    console.log('System: Client connected with ID:', socket.id);

    socket.on('new-flight', (flight) => {
        console.log('Flight {');
        console.log(`  event: 'new-flight',`);
        console.log(`  time: ${new Date().toLocaleString()}`);
        console.log('Details:', flight, '}');
        io.emit('new-flight', flight);
    });

    socket.on('arrived', (flight) => {
        console.log('Flight {');
        console.log(`  event: 'arrived',`);
        console.log(`  time: ${new Date().toLocaleString()}`);
        console.log('Details:', flight, '}');
    });
});

airlineSocket.on('connection', (socket) => {
    socket.on('took-off', flight => {
        console.log('Flight {');
        console.log(`  event: 'took-off',`);
        console.log(`  time: ${new Date().toLocaleString()}`);
        console.log('Details:', flight, '}');
        socket.emit('arrived', flight);
    });
});





