import {Injectable} from 'angular2/core';

@Injectable()
export class SocketHandlerService {
    socket : any;
    initialize(){
        new Promise( (resolve, reject) => {
            this.socket = io.connect(window.location.origin, {
                path: window.location.pathname + 'socket.io'
            });
            if(this.socket) resolve();
            else reject();
        }).then(this._handleSocketConnectSuccess(), this._handleSocketConnectFail);
    }
    _handleSocketConnectSuccess(){
        this._createSocketListeners();
    }
    _handleSocketConnectFail(){
        console.log('Socket has failed to connect to server!');
    }
    _createSocketListeners(){
        this.socket.on('event', function (message) {
            console.log(message);
        });
        this.socketEmit('event', { message: 'Message O.o' });
    }
    socketEmit(event, message){
        this.socket.emit(event, message);
    }
}