import Fractral from './Fractal'
import * as _pixi from 'pixi.js'

export default class PixiShape extends Fractral {
  //Test PIXI Object
  public init(): void {
    const graphics = new _pixi.Graphics();
    graphics.beginFill(0xDE3249);
    graphics.drawRect(50, 50, 100, 100);
    graphics.endFill();
    this.getMainScene.getPixiScene.stage.addChild(graphics);
  }
}