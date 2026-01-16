import { IncomingMessage, OutgoingMessage, Signal } from "@domotics-app/lib";
import { compose } from "ramda";
import { Server as IoServer, Socket } from "socket.io";

export class Server {
  #io: IoServer;
  #isRunning: boolean;

  #socketListener: (socket: Socket) => Socket;

  constructor(port: number) {
    this.#io = new IoServer(port, {
      cors: {
        origin: "*",
        methods: ["GET", "POST"],
      },
    });
    this.#socketListener = (socket: Socket) => socket;
    this.#isRunning = false;
  }

  /**
   * Execute all functions previously composed throught on() methods
   * ```ts
   * server
   *  .on(Signal.CHANGE, (message) => {
   *  console.log(message);
   * })
   *  .run();
   * ```
   */
  run() {
    if (!this.#isRunning) {
      this.#io.on("connection", (socket) => {
        this.#socketListener(socket);
      });
      this.#isRunning = true;
    }
  }

  on(signal: Signal, listener: (message: IncomingMessage) => unknown) {
    this.#socketListener = compose(
      (socket: Socket) => socket.on(signal, listener),
      this.#socketListener
    );

    return this;
  }

  emit(signal: Signal, message: OutgoingMessage) {
    return this.#io.emit(signal, message);
  }
}