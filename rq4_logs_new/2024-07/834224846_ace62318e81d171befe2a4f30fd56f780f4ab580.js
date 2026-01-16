const express = require('express');
const next = require('next');

const port = parseInt(process.env.PORT, 10) || 3000;
const dev = process.env.NODE_ENV !== 'production';
const app = next({ dev });
const handle = app.getRequestHandler();

app.prepare().then(() => {
    const server = express();

    // Aqui você pode adicionar rotas personalizadas se necessário

    server.all('*', (req, res) => {
        return handle(req, res);
    });

    // Escute em todas as interfaces de rede
    server.listen(port, '0.0.0.0', err => {
        if (err) throw err;
        console.log(`> Ready on http://0.0.0.0:${port}`);
    });
});