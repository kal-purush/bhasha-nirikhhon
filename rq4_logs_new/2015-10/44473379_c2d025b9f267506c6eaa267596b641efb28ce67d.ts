///<reference path='typings/tsd.d.ts' />

import * as io from 'socket.io';
import ChannelService from './lib/ChannelService';

class Socket {
    private socket: SocketIO.Server;

    constructor(server) {
        this.socket = io.listen(server);
        this.socket.on('connect', function(socket) {
            socket.on('subscribe', channelName => {
                socket.join(channelName);
            });
            socket.on('unsubscribe', channelName => {
                socket.leave(channelName);
            });
        });

        ChannelService.on('action', (channelName, action) => {
            this.socket.to(channelName).emit('action', {
                channelName: channelName,
                action: action
            });
        });
    }
}

export function connect(server) {
    return new Socket(server);
}