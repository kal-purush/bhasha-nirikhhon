import * as express from "express";
import * as socketIo from "socket.io";
import { createServer } from "http";
import { attachControllers } from "@decorators/socket";
import ShipController from "./Ship";

const PORT = process.env.NODE_ENV === "test" ? 33333 : process.env.PORT || 8080;

export class App {
  app;
  server;
  io: SocketIO.Server;

  constructor() {
    this.app = express();
    this.server = createServer(this.app);
    this.io = socketIo(this.server);

    this.configure();
    attachControllers(this.io, [ShipController]);
  }

  start() {
    this.server.listen(PORT, () => {
      console.log("Listen on " + PORT);
    });
  }

  private configure(): void {
    this.app.use(express.static(__dirname + "/../public"));
    this.app.get("/", (req, res) => {
      res.sendFile("index.html");
    });
  }
}