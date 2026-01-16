import { Injectable } from '@angular/core';
// sudo typings install dt~sockjs-client --global
// sudo typings info dt~sockjs-client --versions
import SockJS                  from 'sockjs-client';
import BaseEvent                    = __SockJSClient.BaseEvent;
import SockJSClass                  = __SockJSClient.SockJSClass;
import { Subject }                  from 'rxjs/Subject';

@Injectable() 
export class SocketService {
    private sock:SockJSClass;// = SockJS ("http://localhost:9999/echo", null, {});
    private openEventSubj = new Subject<any>();
    private messageEventSubj = new Subject<any>();
    private closeEventSubj = new Subject<any>();
  
    public openEvent;// = this.openEventSubj.asObservable();
    public messageEvent;// = this.messageEventSubj.asObservable();
    public closeEvent;// = this.closeEventSubj.asObservable();

    constructor() {
        this.openEvent = this.openEventSubj.asObservable();
        this.messageEvent = this.messageEventSubj.asObservable();
        this.closeEvent = this.closeEventSubj.asObservable();
        this.sock = SockJS("http://localhost:9999/echo", null, {});
        this.sock.onopen = this.onOpen;
        this.sock.onmessage = this.onMessage;
        this.sock.onclose = this.onClose;
    }

    public send(message:any) {
        this.sock.send(message);
    }

    private onOpen(oe:__SockJSClient.OpenEvent) {
        console.log(oe);
        try {
            if (this.openEventSubj === undefined) {
                this.openEventSubj = new Subject<any>();
                this.openEvent = this.openEventSubj.asObservable();
            }
            this.openEventSubj.next(oe);
        } catch (ex) {
            console.log(ex);
        }
    }  

    private onMessage(me:__SockJSClient.MessageEvent) {
        console.log("onMessage " + me.data.toString());
        try {
            if (this.messageEventSubj === undefined) {
                this.messageEventSubj = new Subject<any>();
                this.messageEvent = this.messageEventSubj.asObservable();
            }
            this.messageEventSubj.next(me);
        } catch (ex) {
            console.log(ex);
        }
    }

    private onClose(ce:__SockJSClient.CloseEvent) {
        console.log("closed");
        try {
            if (this.closeEventSubj === undefined) {
                this.closeEventSubj = new Subject<any>();
                this.closeEvent = this.closeEventSubj.asObservable();
            }
            this.closeEventSubj.next(ce);
        } catch(ex) {
            console.log(ex);
        }
    }

}