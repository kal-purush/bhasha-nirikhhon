import * as io from 'socket.io-client';
import config from '../config';

var socket;
var reconnectionAttempt = 1;

function connect() {
    socket = io.connect(config.host, { 'force new connection': true });
    setup();
}

function setup() {
    socket.on('connect', () => {
        console.log('+++ Socket.io connected');
        socket.emit('auth', config.id, (data) => {
            if (!data.success) {
                console.log('--- FATAL: Authentication fail because of "%s"', data.msg);
                process.exit(1);
            }
        });
    });

    socket.on('error', (err) => {
        console.log('--- Socket.io error', err, socket.connected);
        process.exit(1);
    });

    socket.on('disconnect', () => {
        console.log('--- Socket.io disconnected');
        process.exit(1);
    });
}

function sendData(data): Promise<any> {
    return new Promise<any>((resolve, reject) => {
        if (!socket.connected)
            return reject(new Error('Socket disconnected'));

        socket.emit('log', data);
        resolve();
    });
}

connect();

export {sendData};