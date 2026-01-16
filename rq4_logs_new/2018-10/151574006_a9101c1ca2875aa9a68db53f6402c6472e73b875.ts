import {
  Controller,
  Event,
  Connect,
  Socket,
  Args,
  Disconnect,
  IO
} from "@decorators/socket";
import { Injectable } from "@decorators/di";
import { ShipService } from "./Service";

@Injectable()
@Controller("/")
export class ShipController {
  constructor(public shipService: ShipService) {}

  @Connect()
  public onConnect(@Socket() socket: SocketIO.Socket) {
    console.log("User connected ->", socket.id);

    socket.broadcast.emit(
      "newPlayer",
      this.shipService.createPlayer(socket.id)
    );
    socket.emit("currentPlayers", this.shipService.getPlayers());
    socket.emit("starLocation", this.shipService.getStar());
    socket.emit("scoreUpdate", this.shipService.getScores());
  }

  @Disconnect()
  public onDisconnect(@Socket() socket: SocketIO.Socket): void {
    console.log("User disconected -->", socket.id);

    this.shipService.removePlayer(socket.id);
    socket.broadcast.emit("userDisconnect", socket.id);
  }

  @Event("playerMovement")
  onPlayerMovement(@Args() movementData, @Socket() socket: SocketIO.Socket) {
    socket.broadcast.emit(
      "playerMoved",
      this.shipService.updatePlayer(socket.id, movementData)
    );
  }

  @Event("starCollected")
  onStarCollected(
    @IO() io: SocketIO.Server,
    @Socket() socket: SocketIO.Socket
  ) {
    io.emit("starLocation", this.shipService.generateStar());
    io.emit("scoreUpdate", this.shipService.addScore(socket.id));
  }
}