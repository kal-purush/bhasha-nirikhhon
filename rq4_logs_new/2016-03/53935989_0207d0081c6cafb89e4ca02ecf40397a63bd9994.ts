
'use strict';

import {WebSocket} from "ws";

var WebSocketServer = WebSocket.Server;

var server = new WebSocketServer({ port: port });

server.on('connection', ws => {
	ws.on('message', message => {
		try {
			var userMessage: models.UserMessage = new models.UserMessage(message);
			broadcast(JSON.stringify(userMessage));
		} catch (e) {
			console.error(e.message);
		}
	});
});

function broadcast(data: string): void {
	server.clients.forEach(client => {
		client.send(data);
	});	
};

console.log('Server is running on port', port);