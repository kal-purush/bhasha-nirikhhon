import {Injectable} from '@angular/core';
import {Observable, Subject} from 'rxjs';
import {WebsocketService} from './websocket.service';
import {Telemetry} from './telemetry';
import {map} from 'rxjs/operators';

const WS_URL = 'ws://' + location.host + '/ws';

@Injectable({
  providedIn: 'root'
})
export class RovStateService {

  public state: Subject<Telemetry>;

  constructor(websocket: WebsocketService) {
    this.state = <Subject<Telemetry>>websocket.connect(WS_URL).pipe(
      map(response => {
        return {switchState: response.switchState};
      })
    );
  }
}