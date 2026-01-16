import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import { Observer } from 'rxjs/Observer';

import * as socketIo from 'socket.io-client';
import {Analysis} from "./models/analysis";

const SERVER_URL = 'http://localhost:3000';

@Injectable()
export class SocketService {
  private socket;

  public initSocket(priviledge: number): void {
    this.socket = socketIo(SERVER_URL);
    this.socket.on('connect', conn => {
      this.socket.emit('room', { priviledge: priviledge });
    });
  }

  public onMessage(): Observable<Analysis> {
    return new Observable<Analysis>(observer => {
      this.socket.on('message', (data: Analysis) => observer.next(data));
    });
  }
}