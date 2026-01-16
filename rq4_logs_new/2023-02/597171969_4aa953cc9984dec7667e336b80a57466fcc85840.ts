import { PositionType } from "./types";
import { context, canvasErrorString } from "./main";

export class Pellet {
  position: PositionType;
  radius: number;
  constructor({ position }: { position: PositionType }) {
    this.position = position;
    this.radius = 3;
  }

  draw() {
    if (!context) {
      console.error(canvasErrorString);
      return;
    }
    context.beginPath();
    context.arc(this.position.x, this.position.y, this.radius, 0, Math.PI * 2);
    context.fillStyle = "white";
    context.fill();
    context.closePath();
  }
}