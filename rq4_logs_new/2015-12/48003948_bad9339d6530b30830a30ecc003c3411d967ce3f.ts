///<reference path="../typings/tsd.d.ts" />

import Mouse = require('./Mouse');
import * as http from 'http';
import socket = require('socket.io-client');
import debug = require('debug');
import screenres = require('screenres');

let log = debug('remoteControl:Client');

export module RemoteHub{
	
	export interface ClientScreen {
		width: number;
		height: number;
	}
	
	export class RemoteClient {
		
		io: SocketIOClient.Socket;
		screen: ClientScreen = <ClientScreen>{};
		app: http.Server;
		
		constructor(url: string){
			
			log('Client creation');
			
			let self = this;
			this.io = socket(url);
			
			let res = screenres.get();
			this.screen.height = res[0];
			this.screen.width = res[1];
			
			this.io.on('MouseMove', function(event: Mouse.MouseEvent){
				self.screen.width += event.xDelta;
				self.screen.height += event.yDelta;
				
				log('Mouse Position: %sx%s', self.screen.width, self.screen.height);
			});
		}
	}
}