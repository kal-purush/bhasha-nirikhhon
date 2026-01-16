import { RoomCreate, type MessageCreate } from "@sendhelp/core";
import { Server } from "socket.io";
import express from "express";
import http from "http";
import { messageCreate, roomGetAll, roomCreate } from "./usecases";

const app = express();

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "http://localhost:3000",
    methods: ["GET", "POST"],
  },
});

io.on("connection", async (socket) => {
  socket.on("message_create", async (body: MessageCreate) => {
    const username = socket.handshake.auth.username;
    console.log(`[${username}](${body.roomName}): ${body.content}`);
    const message = await messageCreate(body, username);
    io.emit("message_create", message);
  });

  socket.on("room_create", async (body: RoomCreate) => {
    const room = await roomCreate(body);
    if (!room) {
      return;
    }
    io.emit("room_create", room);
  });

  socket.on("ready", async () => {
    const username = socket.handshake.auth.username;
    console.log(`[${username}]: ready`);
    const allRooms = await roomGetAll();
    socket.emit("room_get_all", allRooms);
  });
});

io.listen(8000);