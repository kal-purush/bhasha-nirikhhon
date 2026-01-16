import ControllerInterface = require('./ControllerInterface');
import Snake = require('../EntityGroup/Snake');
import GameEntityGroupInterface = require('../EntityGroup/GameEntityGroupInterface')
import Direction = require('./../Direction');



class SnakeController implements ControllerInterface {

  protected direction:Direction = Direction.RIGHT;

  constructor(protected snake:Snake) {
  }


  bootstrap() {

  }

  onTick() {
    this.moveSnakeForward();
  }

  protected moveSnakeForward():void {
    this.snake.forEach(segment => {

    });
  }
}

export = SnakeController;