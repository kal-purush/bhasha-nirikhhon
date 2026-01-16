import { MovingObject } from './canvas.moving.object';

export class Character extends MovingObject{
  public update() {
    this.newPos();
    this.ctx.fillStyle = this.color;
    this.ctx.fillRect(this.x, this.y, this.width, this.height);
  }

  public moveup() {
    if (this.y >= 600) {
      this.gravitySpeed = -15;
    }
    // this.speedY = - 3;
  }

  public movedown() {
    this.speedY = 2;
  }

  public moveleft() {
    this.speedleftX = -3;
  }

  public moveright() {
    this.speedrightX = +3;
  }

  public stop() {
    this.speedleftX = 0;
    this.speedrightX = 0;
  }
}