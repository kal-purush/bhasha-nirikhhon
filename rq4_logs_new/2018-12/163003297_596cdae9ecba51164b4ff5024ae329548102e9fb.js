const WebSocket = require('ws');
const WebSocketServer = WebSocket.Server;

const wss = new WebSocketServer({ port: 8181 });

wss.on('connection', function(ws, req) {
    const ip = req.connection.remoteAddress.replace('::ffff:', '');
    console.log('client connected', ip);
    ws.send(`你（${ip}）已加入群聊，快和大家打个招呼吧~`);
    ws.on('message', function(message) {
        wss.clients.forEach((client) => {
            if (client !== ws && client.readyState === WebSocket.OPEN) {
                // const clientIp = client._socket._peername.address.replace('::ffff:', '');
                client.send(`${ip}: ${message}`);
            }
        })
    });
});