import SocketIOClient from 'socket.io-client';
import { CallbackManager } from '../../app/lib/CallbackManager'

export class ClientSocket {
  private socket: SocketIOClient.Socket;
  private callbackManager: CallbackManager;

  constructor(socket: SocketIOClient.Socket) {
    this.socket = socket;

    this.callbackManager = new CallbackManager((data) => {
      console.log(data);
      this.socket.emit('c2e_Exec', data);
    }, 'clicmid');

    this.socket.on('s2c_ResultExec', (data) => {
      console.log('resultexec', data);
      this.callbackManager.handleRecieve(data, data.continue);
    });
  }

  generateForPostExec(recieve: (data: any) => void): (data: any) => void {
    return this.callbackManager.multipost(recieve);
  }

  getSocket(): SocketIOClient.Socket {
    return this.socket;
  }

}