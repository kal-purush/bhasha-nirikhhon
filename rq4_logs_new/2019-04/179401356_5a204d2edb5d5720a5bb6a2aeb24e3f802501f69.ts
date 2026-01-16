import * as SocketIO from 'socket.io';
import * as SocketJWT from 'socketio-jwt';
import * as chat from '../http/controllers/chat';
import * as jwt from 'jsonwebtoken';

class Socket {
    private server: any;
    private io: any;

    constructor(server: any) {
        this.server = server;
        this.io = SocketIO(this.server);
        this.config();
    }

    private config() {
        var io = this.io;

        io.on('connection',
            SocketJWT.authorize({
                secret: 'secret',
                required: false
            })
        ).on('authenticated', function (socket: any) {

            socket.on('chatAll', function (data: any) {
                data.authID = socket.decoded_token.id;
                chat.chatAll(io, socket, data);
            })
        });
    }

    public getServer() {
        return this.server;
    }
}

export default Socket