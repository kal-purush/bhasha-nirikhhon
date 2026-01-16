
import $ = require('jquery');
import State = require('../State');
import Game = require('../Game');

class PlayState implements State {
  name: string = "PlayState";

  constructor() {

  }

  enter(): void {
    console.log("Play - enter");

    let circle = new createjs.Shape();
    circle.graphics.beginFill("#0099ff").drawCircle(0, 0, 50);
    circle.x = 100;
    circle.y = 100;
    Game.stage.addChild(circle);

    Game.stage.update();
  }

  exit(): void {

  }

  update(event: createjs.Event): void {
    $('.thing').text(Math.round(createjs.Ticker.getMeasuredFPS()));
  }
}

export = PlayState;