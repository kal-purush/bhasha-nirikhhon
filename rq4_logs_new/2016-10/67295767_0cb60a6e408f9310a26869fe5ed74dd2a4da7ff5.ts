import * as express from "express";
import * as http from "http";
import * as socketio from "socket.io";

import * as partyManager from "./party/party-manager";
import * as playerStore from "./player/player-store";

let app = express();
let server = (<any>http).Server(app);
let io = socketio(server);

app.set('port', process.env.PORT || 3000);
app.use(express.static('public'));

app.get('/reset', (req, res) => {
  console.log("Resetting...");
  partyManager.default.reset();
  res.send("reset done!");
});

partyManager.default.onGameOver.subscribe(winner => {
  io.emit('gameover', winner);
});
partyManager.default.onUpdate.subscribe(() => {
  console.log('update all players!');
  io.emit('update-all', playerStore.default.get());
});
partyManager.default.onCardsBet.subscribe(cards => {
  console.log('send cards to bet!');
  io.emit('cards-bet', cards);
});
partyManager.default.onBetsReveled.subscribe(bets => {
  console.log('reveled all bets!');
  io.emit('bets-reveled', bets);
});

io.on('connection', (socket) => {
  socket.on('join', (playerId: string, playerName: string) => {
    partyManager.default.join({
      id: playerId,
      name: playerName
    });
  });
  //socket.on('disconnect', function () { playerManager.handleDisconnect(io, socket); });
  //socket.on('reconnect-player',(playerId,playerName)=>{ playerManager.handleReconnect(io, socket, playerId, playerName); });
  socket.on('pick-card', (playerId: string, cardId: number) => {
    partyManager.default.pickCard(playerId, cardId);

  });
  socket.on('pick-bet', (playerId: string, cardId: number) => {
    partyManager.default.betCard(playerId, cardId);
  });
  socket.on('discard-card', (playerId: string, cardId: number) => {
    partyManager.default.discardCard(playerId, cardId);
  });
  socket.on('rename-player', (playerId: string, newName: string) => {
    partyManager.default.renamePlayer({
      id: playerId,
      name: newName
    });
  });
});

server.listen(app.get('port'), () => {
  console.log(`listening on :${app.get('port')}`);
});