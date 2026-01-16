import type { NextApiRequest, NextApiResponse } from "next";
import { Server, Socket } from "socket.io";
import { Socket as NetSocket } from "net";
import { writeFile } from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";

type EnhancedSocket = NetSocket & { server: { io?: Server } };

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  const socket = res.socket as EnhancedSocket | null;

  if (!socket || !socket.server) {
    console.error("Socket or server is undefined");
    res.status(500).send("Server error");
    return;
  }

  if (socket.server.io) {
    console.log("Socket is already running");
    socket.emit("log", { text: "Connection is ready" });
  } else {
    console.log("Socket is initializing");
    const io = new Server(socket.server as any);
    socket.server.io = io;

    io.on("connection", (socket) => {
      socket.emit("log", { text: "Connection is ready" });

      socket.on("start-check", async (content) => {
        console.log("got check request", content);
        // socket.emit("log", content);
        // TODO
      });

      socket.on("chat", async (content) => {
        try {
          console.log("got chat request", content);
        } catch (err) {
          console.error(err);
          socket.emit("message", { text: "Sorry, I ran into an error" });
        }

        socket.emit("message", { text: "Work on this file is complete" });
        socket.emit("finished");
      });
    });
  }
  res.end();
}