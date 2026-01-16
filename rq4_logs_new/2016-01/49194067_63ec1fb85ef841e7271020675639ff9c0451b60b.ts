/// <reference path="../typings/express-ws/express-ws.d.ts" />
import ExpressWs = require('express-ws');


module WebSocketManager {

	export class WebSocketClientManager {
		
		private static idCounter = 0;
		
		private pagesAndClients: PagesAndClients;
		
		public constructor() {
			this.pagesAndClients = {};
		};
		
		public addClient(page: string, client: ExpressWs.ExpressWebSocket) {
			if (!this.pagesAndClients.hasOwnProperty(page)) {
				this.pagesAndClients[page] = [];
			}
			
			const clientWithId = client as ExpressWebSocketWithId;
			
			clientWithId.id = WebSocketClientManager.idCounter++;
			
			clientWithId.on('close', () => {
				this.tryRemoveClient(page, clientWithId);
			});
			clientWithId.on('error', () => {
				this.tryRemoveClient(page, clientWithId);
			});
			
			this.pagesAndClients[page].push(clientWithId);
		};
		
		public tryRemoveClient(page: string, client: ExpressWebSocketWithId): boolean {
			try {
				const idx = this.pagesAndClients[page]
					.map((client, idx) => { return { client, idx }; })
					.filter(obj => obj.client.id === client.id)[0].idx;
				
				this.pagesAndClients[page].splice(idx, 1); // delete this client
				if (this.pagesAndClients[page].length === 0) {
					delete this.pagesAndClients[page];
				}
				return true;
			} catch (e) { }
			return false;
		};
		
		public notifyAllClients(page: string, msg?: string) {
			if (this.pagesAndClients.hasOwnProperty(page)) {
				this.pagesAndClients[page].forEach(c => setTimeout(() => {
					c.send(msg || 'notify');
				}, 0));
			}
		};
	};
	
	export interface PagesAndClients {
		[key: string]: ExpressWebSocketWithId[];
	};
	
	export interface ExpressWebSocketWithId extends ExpressWs.ExpressWebSocket {
		id: number;
	};
};




// Export this singleton-instance
export = new WebSocketManager.WebSocketClientManager();