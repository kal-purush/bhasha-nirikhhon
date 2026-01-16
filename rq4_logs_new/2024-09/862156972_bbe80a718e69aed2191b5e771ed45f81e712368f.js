const express = require('express');
const http = require('http');
const socketIo = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = socketIo(server);

app.use(express.static('public'));

// Rota principal
app.get('/', (req, res) => {
    res.sendFile(__dirname + '/public/index.html');
});

io.on('connection', (socket) => {
    console.log('Usuário conectado');
    
    // Envia uma mensagem quando o cliente se conecta
    socket.emit('message', 'Bem-vindo ao chat!');

    // Recebe mensagens do cliente e as envia para todos
    socket.on('chat message', (msg) => {
        io.emit('chat message', msg);
    });

    // Lida com a desconexão do usuário
    socket.on('disconnect', () => {
        console.log('Usuário desconectado');
    });
});

// Inicializa o servidor na porta 8888
const PORT = process.env.PORT || 8888;
server.listen(PORT, () => {
    console.log(`Servidor rodando na porta ${PORT}`);
});