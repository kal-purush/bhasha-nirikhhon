import { Injectable } from '@angular/core';

import { Map } from '../../../shared/models/map';

const Primus = require('../primus.gen');

export const settings = window.location.hostname === '' ?
  { port: 80,   protocol: 'http', hostname: '' } :
  { port: 8080, protocol: 'http', hostname: window.location.hostname };

@Injectable()
export class PrimusService {

  socket: any;
  outstandingCallbacks: any;

  constructor() {
    this.outstandingCallbacks = {};
  }

  initSocket() {
    if(this.socket) { return; }
    this.socket = Primus.connect(`${settings.protocol}://${settings.hostname}:${settings.port}`, {
      reconnect: {
        min: 500,
        retries: 50,
        factor: 2
      }
    });

    this.socket.on('data', data => {
      console.log(data);
      if(!this[`handle${data.updateType}`]) { return; }
      this[`handle${data.updateType}`](data);

      if(!this.outstandingCallbacks[data.event]) { return; }
      this.outstandingCallbacks[data.event](data);
      this.outstandingCallbacks[data.event] = null;
    });
  }

  handleMap({ data }: { data: Map }) {
    console.log(data);
  }

  emit(event: string, data?: any, callback?: () => {}) {
    this.outstandingCallbacks[event] = callback;
    this.socket.emit(event, data);
  }

  requestMap() {
    this.emit('plugin:map:request');
  }
}