import { Server } from "socket.io";
import { Server as HTTPServer } from "http";
import { ServerToClientEvents, ClientToServerEvents } from "../sockets/Types/socketTypes"; // Import types

let io: Server<ClientToServerEvents, ServerToClientEvents>;

export function initSocket(httpServer: HTTPServer) {
  io = new Server<ClientToServerEvents, ServerToClientEvents>(httpServer, {
    cors: {
      origin: process.env.CLIENT_URL, // Frontend URL
      methods: ["GET", "POST", "PUT", "PATCH", "DELETE"],
    },
  });

  io.on("connection", (socket) => {
    console.log(`User connected: ${socket.id}`);

    // Handle events for users and admins
    require("./socket/index").registerSocketEvents(io);  // Register socket events

    // Clean up on disconnect
    socket.on("disconnect", () => {
      console.log(`User disconnected: ${socket.id}`);
    });
  });

  return io;
}

export function getSocketIO(): Server<ClientToServerEvents, ServerToClientEvents> {
  if (!io) {
    throw new Error("Socket.IO is not initialized. Call initSocket first.");
  }
  return io;
}