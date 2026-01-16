import { Injectable } from '@angular/core';
import { Observable, Observer, ReplaySubject } from 'rxjs';
import { map, switchMap } from 'rxjs/operators';

const WS_SERVER_URL = 'ws://e-sport-dev.herokuapp.com/';

@Injectable({ providedIn: 'root' })
export class WebsocketService {
  private ws$ = new ReplaySubject<WebSocket>(1);

  constructor() {
    const socket = new WebSocket(WS_SERVER_URL);
    socket.onopen = () => {
      console.log('Successfully connected to the WebSocket at', WS_SERVER_URL);
      this.ws$.next(socket);
    };
  }

  public listen(): Observable<MessageEvent> {
    return this.ws$.pipe(
      switchMap(socket => {
        return new Observable((subscriber: Observer<MessageEvent>) => {
          socket.onmessage = message => subscriber.next(message);
          socket.onerror = error => subscriber.error(error);
          socket.onclose = () => subscriber.complete();
          return () => socket.close();
        });
      }),
      map((event: MessageEvent) => event.data)
    );
  }

  public send(data: any): void {
    this.ws$.subscribe(socket => {
      socket.send(JSON.stringify(data));
    });
  }
}