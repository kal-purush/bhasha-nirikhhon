const FRONTEND_URL = process.env.ORIGIN || "https://place-app.netlify.app/" 

const socket = (server) => {

    const io = require('socket.io')(server, {
        cors: { 
            origin: [FRONTEND_URL]
        }
    })


    io.on('connect',  (socket) => {
  
        console.log('Se ha conectado un usuario')

        socket.on('chat_message', (data) => {
                console.log("este es el mensaje que te viene del chat", data)

            io.emit('chat_message', (data))

        });

    //     socket.on('disconnect', function () {
    //         console.log(`A user disconnected)`);
    //     });


    //     socket.on('connect_error', function (error) {
    //         console.log('Socket connection error:', error);  
    //     });

    });



}

module.exports = socket

