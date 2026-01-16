import { IPlayer } from "./interfaces/IPlayer";
import { IScore } from "./interfaces/IScore";
import { IStar } from "./interfaces/IStar";

export class GameSocket {
  players: IPlayer[];
  scores: IScore[];
  star: IStar;
  io: SocketIO.Server;

  constructor(io: SocketIO.Server) {
    this.players = [];
    this.scores = [{ team: "blue", quantity: 0 }, { team: "red", quantity: 0 }];
    this.star = {
      x: Math.floor(Math.random() * 700) + 50,
      y: Math.floor(Math.random() * 500) + 50
    };
    this.io = io;

    this.listen();
  }

  private listen() {
    this.io.on("connection", socket => {
      console.log("User connected...");
      const player = {
        rotation: 0,
        x: Math.floor(Math.random() * 700) + 50,
        y: Math.floor(Math.random() * 500) + 50,
        playerId: socket.id,
        team: Math.floor(Math.random() * 2) === 0 ? "red" : "blue"
      };

      this.players.push(player);

      socket.emit("currentPlayers", this.players);
      socket.emit("starLocation", this.star);
      socket.emit("scoreUpdate", this.scores);
      socket.broadcast.emit("newPlayer", player);

      socket.on("playerMovement", movementData => {
        const player = this.players.find(p => p.playerId === socket.id);

        socket.broadcast.emit("playerMoved", {
          ...player,
          x: movementData.x,
          y: movementData.y,
          rotation: movementData.rotation
        });
      });

      socket.on("starCollected", () => {
        const player = this.players.find(p => p.playerId === socket.id);

        const scoresUpdated = this.scores.map(s => {
          if (s.team === player.team)
            return { ...s, quantity: (s.quantity += 10) };
          return s;
        });

        const star = {
          x: Math.floor(Math.random() * 700) + 50,
          y: Math.floor(Math.random() * 500) + 50
        };

        this.io.emit("starLocation", star);
        this.io.emit("scoreUpdate", scoresUpdated);
      });

      socket.on("disconnect", () => {
        console.log("user disconected");
        this.players = this.players.filter(p => p.playerId !== socket.id);
        this.io.emit("disconnect", socket.id);
      });
    });
  }
}