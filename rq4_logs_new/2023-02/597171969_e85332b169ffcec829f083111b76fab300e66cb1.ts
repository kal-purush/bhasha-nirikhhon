import { BoundaryConstructor } from "./types";
import { context, canvasErrorString } from "./main";

export class Boundary {
  position;
  width: number;
  height: number;
  static cellWidth = 40;
  static cellHeight = 40;
  image;
  constructor({ position, image }: BoundaryConstructor) {
    this.position = position;
    this.width = 40;
    this.height = 40;
    this.image = image;
  }

  draw() {
    // I'm using a guard here to account for the
    // context possibly being null. Ideally
    // I'd have a custom error or function that
    // introduces a visual error state to the game.
    if (!context) {
      console.error(canvasErrorString);
      return;
    }
    // context.fillStyle = "blue";
    // context.fillRect(this.position.x, this.position.y, this.width, this.height);
    context.drawImage(this.image, this.position.x, this.position.y);
  }
}