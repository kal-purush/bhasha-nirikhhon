class Socket {
    constructor(webSocketType='websocket') {
        this.webSocketType = webSocketType;
        this.ws = null;
        this.onmessage = () => {}
    }

    connect () {
        switch (this.webSocketType) {
            case 'websocket':
                this.ws = new WebSocket('ws://localhost:5000');
                this.ws.onmessage = this.onmessage;
                break;
        
            case 'longPolling':
                
                break;
        
            case 'mercure':
                
                break;
        }
    }
    
    send (data) {
        switch (this.webSocketType) {
            case 'websocket':
                if (this.ws.readyState == WebSocket.OPEN) {
                    this.ws.send(data)
                }
                break;
        
            case 'longPolling':
                
                break;
        
            case 'mercure':
                
                break;
        }
    }
}

module.exports = { Socket }