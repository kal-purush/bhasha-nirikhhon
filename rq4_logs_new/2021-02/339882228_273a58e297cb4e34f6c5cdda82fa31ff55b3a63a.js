export class WebSocketClient {
	
	constructor() {
		
	}
	
	connect() {
		console.log("CONNECTING");
		this.webSocket = new WebSocket('ws://localhost:8081/gamebackend/websocket');
		
		this.webSocket.onopen = (event) => {
			user.id = uuid();
		}
		
		this.webSocket.onclose = (event) => {
			console.log('onclose');
		}
		
		this.webSocket.onerror = (event) => {
			console.log('onerror');
		}
		
		this.webSocket.onmessage = (event) => {
			console.log('message: ' + JSON.stringify(event.data));
		}
	}
	
	disconnect() {
		this.webSocket.close();
	}
	
	sendMessage(message) {
		console.log('sending message: ' + message);
		this.webSocket.send(message);
	}
}