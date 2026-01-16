/// <reference path="../../typings/tsd.d.ts" />

import _ = require('lodash');
import events = require('events');

import {ChatDataStoreInterface, ERRORS, EVENTS} from './DataStoreInterfaces';

class ChatMemory implements ChatDataStoreInterface {
    private globalChat:string[] = [];

    private emitter:events.EventEmitter = new events.EventEmitter();

    public getGlobalChat(limit:number, callback:(err:Error, allMessages:string[])=>any) {
        limit = limit || 20;

        while (this.globalChat.length > limit && this.globalChat.length > 0) {
            this.globalChat.shift();
        }

        process.nextTick(() => callback(null, _.cloneDeep(this.globalChat)));
    }

    public pushGlobalChat(message:string, callback:(err:Error, message:string)=>any) {
        if (!message) {
            return callback(new Error(ERRORS.CHAT.INVALID_MESSAGE), null);
        }
        this.globalChat.push(message);

        this.emitter.emit(EVENTS.GLOBALCHAT, message);
        process.nextTick(() => callback(null, message));
    }

    public onGlobalChat(callback:(message:string)=>any) {
        this.emitter.on(EVENTS.GLOBALCHAT, callback);
    }
}

export = ChatMemory;