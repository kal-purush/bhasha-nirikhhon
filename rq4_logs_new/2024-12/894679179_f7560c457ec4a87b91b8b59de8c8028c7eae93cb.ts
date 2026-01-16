import { Server } from 'socket.io';
import { Logger } from '../utils/Logger';
import http from 'http';

export const createSocket = (server: http.Server): Server => {
  const io = new Server(server, {
    cors: {
      methods: ['GET', 'POST'],
      origin: ['http://localhost:5173'],
      credentials: true,
    },
  });
  return io;
};

export const socketInit = (io: Server): void => {
  io.on('connection', (socket) => {
    Logger.info(`a user connected with id: ${socket.id}`);
  });

  io.on('disconnect', (socket) => {
    Logger.info(`a user disconnected with id: ${socket.id}`);
  });
};