/// <reference path="../typings/tsd.d.ts" />
import serverModule = require("./serverModule");
import routerModule = require("./routerModule");

class Main{
	constructor(){
		var router: routerModule.Router = new routerModule.Router();
		var serverAPI: serverModule.ServerAPI = new serverModule.ServerAPI(router);
		serverAPI.initServer();
	}
}

var main:Main = new Main();