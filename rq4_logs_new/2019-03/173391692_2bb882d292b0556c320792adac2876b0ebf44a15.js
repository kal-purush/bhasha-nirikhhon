const auth = require('../auth');
const WebSocket = require('ws');

class Client {
    constructor({ username, role, id, socket, ip }) {
        this.id = id;
        this.socket = socket;
        this.ip = ip;
        this.username = username;
        this.role = role;
    }

    get admin() {
        return this.role === 'Admin';
    }

    authenticate(token) {
        const decrypt = auth.verify(token);
        if(decrypt) {
            this.role = decrypt.role;
            this.username = decrypt.username;
        }
        return decrypt;
    }

    unauthenticate() {
        this.username = null;
        this.role = null;
    }

    send(json) {
        if(this.socket && this.socket.readyState === WebSocket.OPEN) {
            this.socket.send(JSON.stringify(json));
        }
    }
}

module.exports = Client;