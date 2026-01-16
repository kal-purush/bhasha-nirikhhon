var game = require("../src/game.js");
var assert = require("assert");

console.log("Test scenario:");
console.log("  1) start new game");
console.log("  2) move player up until they reach the top and win");

game.start();
var count = game.getBoardHeight()-1;
var status = null;
while (count--){
    game.moveUp();
}
status = game.getStatus();
assert.equal(status.player.col, 0, "Player is in first column");
assert.equal(status.player.row, 7, "Player is in last row");
assert.equal(status.isGameWon, true, "Game has been won");
assert.equal(status.activeMines, game.getActiveMines(), "There are mines left");

/*
    Still need to implement tests for hitting mines
 */