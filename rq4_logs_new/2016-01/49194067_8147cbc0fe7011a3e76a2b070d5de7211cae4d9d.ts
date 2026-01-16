/// <reference path="../express/express.d.ts" />

declare module ExpressWs {
	export interface Application { }
}


declare module "express-ws" {
	
	import * as express from "express";
	
	function e(app: Express.Application): e.ExpressWsApplication;
	
	module e {
		export interface ExpressWebSocket extends WebSocket {
			on: (event: string, callback: ExpressWsEventCallback) => void;
		}
		
		export interface ExpressWsCallback {
			(exWs: ExpressWebSocket, req: Express.Request): void;
		}
		
		export interface ExpressWsEventCallback {
			/**
			 * We don't care for the return value or if it accepts a msg:
			 */
			(msg?: string): void|any;
		}
		
		export interface WsApplication extends express.Application {
			ws: (path: string, callback: ExpressWsCallback) => ExpressWsApplication;
		}
		
		export interface ExpressWsApplication {
			app: WsApplication;
		}
	}
	
	export = e;
}