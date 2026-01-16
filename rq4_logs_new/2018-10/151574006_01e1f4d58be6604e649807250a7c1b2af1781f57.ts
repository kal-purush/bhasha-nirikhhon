import { IScore } from "../interfaces/IScore";
import {
  Controller,
  Event,
  Connect,
  Socket,
  ServerMiddleware,
  IO_MIDDLEWARE,
  Args,
  Disconnect,
  IO
} from "@decorators/socket";
import { Injectable, Container } from "@decorators/di";
import { ShipGameService } from "./Service";

class GlobalMiddleware implements ServerMiddleware {
  public use(io, socket, next) {
    console.log("ControllerMiddleware");
    next();
  }
}

@Injectable()
@Controller("/")
export class ShipGameController {
  scores: IScore[];

  constructor(public shipGameService: ShipGameService) {
    this.scores = [{ team: "blue", quantity: 0 }, { team: "red", quantity: 0 }];
  }

  @Connect()
  public onConnect(@Socket() socket: SocketIO.Socket) {
    console.log("User connected ->", socket.id);

    socket.broadcast.emit(
      "newPlayer",
      this.shipGameService.createPlayer(socket.id)
    );
    socket.emit("currentPlayers", this.shipGameService.getPlayers());
    socket.emit("starLocation", this.shipGameService.getStar());
    socket.emit("scoreUpdate", this.shipGameService.getScores());
  }

  @Disconnect()
  public onDisconnect(@Socket() socket: SocketIO.Socket): void {
    console.log("User disconected -->", socket.id);

    this.shipGameService.removePlayer(socket.id);
    socket.broadcast.emit("userDisconnect", socket.id);
  }

  @Event("playerMovement")
  onPlayerMovement(@Args() movementData, @Socket() socket: SocketIO.Socket) {
    socket.broadcast.emit(
      "playerMoved",
      this.shipGameService.updatePlayer(socket.id, movementData)
    );
  }

  @Event("starCollected")
  onStarCollected(
    @IO() io: SocketIO.Server,
    @Socket() socket: SocketIO.Socket
  ) {
    io.emit("starLocation", this.shipGameService.generateStar());
    io.emit("scoreUpdate", this.shipGameService.addScore(socket.id));
  }
}

Container.provide([{ provide: IO_MIDDLEWARE, useClass: GlobalMiddleware }]);