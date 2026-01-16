import { Injectable } from '@angular/core';

@Injectable()
export class GameStateService {


  constructor() { };

  gameState: {
    startingDate,
    endingDate,
    gold: {
      offensePlayer: '',
      defensePlayer: '',
      score: 0,
    }
    black: {
      offensePlayer: '',
      defensePlayer: '',
      score: 0,
    }
  };

  start() {
    this.gameState.startingDate = Date.now();
  }
  end() {
    this.gameState.endingDate = Date.now();
  }
  /*'goldScored' and 'cancelGoldGoal' are a better interface than eg 'incrementGoldScore()'
  Because it is more  future proof (eg adding sensors or recording time will involve other operations than just an increment)
  */
  goldScored() {
    this.gameState.gold.score++;
  };
  cancelGoldGoal() {
    this.gameState.gold.score--;
  };
  goldSwappedPositions() {
    //TODO
  }
  blackScored() {
    this.gameState.black.score++;
  }
  cancelBlackGoal() {
    this.gameState.black.score--;
  }
  blackSwappedPositions() {
    //TODO
  }
  getPlayers() {
    return {gold:  {offense: this.gameState.gold.offensePlayer,
                    defense: this.gameState.gold.defensePlayer},
            black: {offense: this.gameState.black.offensePlayer,
                    defense: this.gameState.black.defensePlayer}}
  }
  getScore() {
    return {gold: this.gameState.gold.score, black: this.gameState.black.score};
  }
  getElapsedTime(): {minutes,  seconds} {
    var currentTime = Date.now();
    var delta = currentTime - this.gameState.startingDate;
    delta /= 1000; //convert to seconds

    return {minutes: Math.floor(delta/60), seconds: delta % 60};
  }
  


}