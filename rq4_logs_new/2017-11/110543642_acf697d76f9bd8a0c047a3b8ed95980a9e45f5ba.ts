import { CanvasPlatform } from '../helpers/canvas.platform';

export class Maps {
  public tatooineBottom: CanvasPlatform;

  public notMovingGameObjects = [
    this.tatooineBottom = new CanvasPlatform(this.ctx, 900, 20, 0, 300, 'blue'),
    this.tatooineBottom = new CanvasPlatform(this.ctx, 500, 20, 0, 400, 'blue'),
    this.tatooineBottom = new CanvasPlatform(this.ctx, 1000, 20, 0, 630, 'blue'),
  ];

  constructor(public ctx) {
    for (const obj of Object.keys(this.notMovingGameObjects)) {
      this.notMovingGameObjects[obj].draw();
    }
  }

  public draw(){

  }

}


