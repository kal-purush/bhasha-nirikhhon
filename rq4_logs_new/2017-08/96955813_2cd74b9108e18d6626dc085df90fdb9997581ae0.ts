import { Injectable } from '@angular/core';

// rx/js imports
import * as Rx from 'rxjs/Rx';

@Injectable()
export class WebSocketService {
    private subject: Rx.Subject<MessageEvent>;

    constructor() { }

    public connect(url): Rx.Subject<MessageEvent> {
        if (!this.subject) {
          this.subject = this.create(url);
          console.log(`Successfully connected: ${url} `);
        }

        return this.subject;
    }

    private create(url): Rx.Subject<MessageEvent> {
        const socket = new WebSocket(url);

        const observable = Rx.Observable.create( (obs: Rx.Observer<MessageEvent>) => {
            socket.onmessage = obs.next.bind(obs);
            socket.onerror = obs.error.bind(obs);
            socket.onclose = obs.complete.bind(obs);
            return socket.close.bind(socket);
        });

        const observer = {
            next: ( data: Object ) => {
                if ( socket.readyState === WebSocket.OPEN ) {
                    socket.send( JSON.stringify(data) )
                }
            }
        }

        return Rx.Subject.create(observer, observable);
    }
}