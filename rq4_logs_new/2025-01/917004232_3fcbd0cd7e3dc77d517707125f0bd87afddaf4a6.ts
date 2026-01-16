import express from 'express';
import http from 'http';
import { Server } from 'socket.io';

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "http://localhost:3000", 
    methods: ["GET", "POST"]
  }
});

interface WhiteboardUpdatePayload {
  roomId: string;
  data: any; 
}

let whiteboardData: { [key: string]: any[] } = {}; 

io.on('connection', (socket) => {
  console.log(`User connected: ${socket.id}`);

  socket.on("join-room", (roomId: string) => {
    socket.join(roomId);
    console.log(`Socket ${socket.id} joined room ${roomId}`);
    
    if (whiteboardData[roomId]) {
      whiteboardData[roomId].forEach((data) => {
        socket.emit("whiteboard-update", data); // Emit existing strokes to the new user
      });
    }
  });

  socket.on("whiteboard-update", ({ roomId, data }: WhiteboardUpdatePayload) => {
    console.log(`Received whiteboard-update for room ${roomId}:`, data);
    
    if (!whiteboardData[roomId]) {
      whiteboardData[roomId] = [];
    }
    whiteboardData[roomId].push(data);

    
    io.to(roomId).emit("whiteboard-update", data); 
  });

  socket.on('down', (roomId: string, { x, y }: { x: number, y: number }) => {
    console.log(`Received 'down' event from user ${socket.id} in room ${roomId}`);
    
    io.to(roomId).emit('ondown', { x, y }); 
  });

  socket.on('disconnect', () => {
    console.log(`User disconnected: ${socket.id}`);
  });
});

server.listen(4000, () => {
  console.log("Server listening on port 4000");
});