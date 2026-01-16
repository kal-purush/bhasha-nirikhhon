/// <reference path="../typings/express/express.d.ts" />
/// <reference path="../typings/express-ws/express-ws.d.ts" />
/// <reference path="../modules/WebSocketClientManager.ts" />

import Express = require('express');
import ExpressWs = require('express-ws');
import WebSocketClientManager = require('../modules/WebSocketClientManager');

export function configure(expressApp: ExpressWs.ExpressWsApplication) {
	/**
	 * This is where Github will be posting to.
	 */
	expressApp.app.post('/webhook/:page', (req: Express.Request, res: Express.Response) => {
		// Notify all clients of the given page with payload
		const payload = req.body && req.body.payload ? req.body.payload : void 0;
		WebSocketClientManager.notifyAllClients(
			decodeURIComponent(req.params['page']), payload);
		res.end();
	});
};