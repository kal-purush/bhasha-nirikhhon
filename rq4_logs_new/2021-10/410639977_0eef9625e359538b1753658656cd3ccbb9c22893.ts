import { Server } from 'socket.io';
import { Server as HttpServer } from 'http';
import { tokenValidationWS } from '@src/helpers/validations';
import { SOCKET_EVT } from '@constants/urls';
import { MESSAGES } from '@constants/messages';
import { TSocket } from './type';
import { Users } from '../User/User.model';
import { User } from '../User/User.controller';

let io: Server = null;

export class Socket {
  io: Server;

  sockets: Array<TSocket>;

  constructor() {
    if (this.io) return null;
    this.io = io;
    this.sockets = [];
  }

  connect = (app: HttpServer): void => {
    io = new Server(app, { cors: { origin: '*' } });
    io.on('connection', async (socket) => {
      console.log(socket);

      this.sockets.push(socket);
      const { userId, isInvalid } = tokenValidationWS(socket.handshake.auth.token, socket);
      if (isInvalid) return socket.emit(SOCKET_EVT.check_auth, MESSAGES.un_autorized);
      User.getUserData(socket);
      await Users.updateOne({ _id: userId }, { $set: { online: true } });
      socket.on('disconnect', async () => {
        await Users.updateOne({ _id: userId }, { $set: { online: false } });
        this.sockets.splice(this.sockets.indexOf(socket), 1);
      });
    });
  };
}