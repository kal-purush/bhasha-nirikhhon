const players = [
  { id: "player-1", name: "Mango", timePlayed: 110, points: 54, online: false },
  { id: "player-2", name: "Poly", timePlayed: 150, points: 60, online: true },
  { id: "player-3", name: "Ajax", timePlayed: 200, points: 67, online: true },
  { id: "player-4", name: "Huston", timePlayed: 405, points: 33, online: true },
  { id: "player-5", name: "Kiwi", timePlayed: 500, points: 89, online: false },
];
console.table(players);

const isAllOnline = players.every((player) => player.online);
console.log("isAllOnline: ", isAllOnline);

const isAllOffline = players.some((player) => player.online);
console.log("isAllOffline: ", isAllOffline);