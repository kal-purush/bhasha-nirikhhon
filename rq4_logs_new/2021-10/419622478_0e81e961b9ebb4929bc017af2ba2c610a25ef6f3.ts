import Server from '../server';
import { UserSocket } from '../types/socket';

const ioHandler = (server: Server) => {
  const { io } = server;

  io.on('connection', (socket: UserSocket) => {
    console.log('User connected');
    console.log(socket.request.user);

    socket.on('disconnect', () => {
      console.log('User disconnect');
    });
  });
};

export default ioHandler;