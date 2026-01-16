/// <reference path="../../typings/tsd.d.ts" />

/// <reference path="../api.d.ts" />

import {ChatDataStoreInterface} from '../datastore/DataStoreInterfaces.ts';
import {ChatRouteControllerInterface} from './Routes.ts';

import RouteErrors = require('./RouteErrors');

class ChatRouteController implements ChatRouteControllerInterface {
    private api:ChatDataStoreInterface = null;

    constructor(api:ChatDataStoreInterface) {
        this.api = api;
    }

    public getMessages(callback:(err:Error, result:ApiResponses.ChatResponse[])=>any) {
        this.api.getGlobalChat(20, callback);
    }

    public postMessage(request:{message:string}, callback:(err:Error, result:ApiResponses.ChatResponse)=>any) {
        if (!request || !request.message) {
            return callback(new Error(RouteErrors.ERROR_INVALID_MESSAGE), null);
        }

        this.api.pushGlobalChat(request.message, callback);
    }
}

export = ChatRouteController;