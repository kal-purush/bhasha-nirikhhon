import * as express from "express";
import * as socketIo from "socket.io";
import { createServer, Server } from "http";

const app = express();
const server = createServer(app);
const io = socketIo(server);
const PORT = process.env.PORT || 8080;

const players = {};
const star = {
  x: Math.floor(Math.random() * 700) + 50,
  y: Math.floor(Math.random() * 500) + 50
};
const scores = {
  blue: 0,
  red: 0
};

app.use(express.static(__dirname + "/../public"));

app.get("/", (req, res) => {
  res.sendFile("index.html");
});

server.listen(PORT, () => {
  console.log("Listen on " + PORT);
});

io.on("connection", socket => {
  console.log("a user connected");
  players[socket.id] = {
    rotation: 0,
    x: Math.floor(Math.random() * 700) + 50,
    y: Math.floor(Math.random() * 500) + 50,
    playerId: socket.id,
    team: Math.floor(Math.random() * 2) === 0 ? "red" : "blue"
  };

  socket.emit("currentPlayers", players);
  socket.emit("starLocation", star);
  socket.emit("scoreUpdate", scores);
  socket.broadcast.emit("newPlayer", players[socket.id]);

  socket.on("playerMovement", movementData => {
    players[socket.id].x = movementData.x;
    players[socket.id].y = movementData.y;
    players[socket.id].rotation = movementData.rotation;

    socket.broadcast.emit("playerMoved", players[socket.id]);
  });

  socket.on("starCollected", () => {
    if (players[socket.id].team === "red") {
      scores.red += 10;
    } else {
      scores.blue += 10;
    }

    star.x = Math.floor(Math.random() * 700) + 50;
    star.y = Math.floor(Math.random() * 500) + 50;
    io.emit("starLocation", star);
    io.emit("scoreUpdate", scores);
  });

  socket.on("disconnect", () => {
    console.log("user disconected");
    delete players[socket.id];
    io.emit("disconnect", socket.id);
  });
});