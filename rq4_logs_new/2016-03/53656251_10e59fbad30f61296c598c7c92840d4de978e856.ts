/// <reference path="memory-classes-interfaces.ts" />

const NUM_CARDS = 12;

$(document).ready(function(){
  let game = new MemoryGame.Game(NUM_CARDS);
});