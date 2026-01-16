import { Injectable } from '@angular/core';
import { webSocket } from 'rxjs/webSocket';

import { environment } from '../../environments/environment';

@Injectable({
  providedIn: 'root',
})
export class WebSocketService {
  constructor() {}

  subject = webSocket('wss://socket.blockcypher.com/v1/btc/test3');
  ping;

  connectWs(address) {
    console.log('connected ws');
    this.subject.subscribe(
      (msg) => console.log(msg),
      (err) => console.log(err),
      () => console.log('complete')
    );

    this.subject.next({
      event: 'unconfirmed-tx',
      address: address,
      token: environment.BC_TOKEN,
    });

    this.ping = setInterval(() => {
      this.subject.next({
        event: 'ping',
      });
    }, 30000);
  }

  closeWs() {
    clearInterval(this.ping);
    this.subject.complete();
    console.log('disconnected ws');
  }
}