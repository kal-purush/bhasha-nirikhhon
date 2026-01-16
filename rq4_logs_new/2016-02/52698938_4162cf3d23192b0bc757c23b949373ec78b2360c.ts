import Entity = require('../Entity');
import Game = require('../Game');
import Util = require('../Util');

class Enemy extends Entity {

  static SIZE = 50;

  shape: createjs.Shape;

  constructor() {
    super();

    this.shape = new createjs.Shape();
    this.shape.graphics.beginFill('#000000').drawRect(0, 0, Enemy.SIZE, Enemy.SIZE);
    this.shape.setBounds(0, 0, Enemy.SIZE, Enemy.SIZE);
    this.addChild(this.shape);

    this.regX = Enemy.SIZE/2;
    this.regY = Enemy.SIZE/2;

    this.addEventListener("added", ()=>( this.added() ));
  }

  added() {
    createjs.Ticker.addEventListener("tick", (event: createjs.TickerEvent)=>( this.update(event) ));
  }

  update(event: createjs.TickerEvent) {
    let deltaTime = event.delta / 1000; // convert to seconds
  }

  getDisplayObject(): createjs.DisplayObject {
    return this.shape;
  }

}

export = Enemy;