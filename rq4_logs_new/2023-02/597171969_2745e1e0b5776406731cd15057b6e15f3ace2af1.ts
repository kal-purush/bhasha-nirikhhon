import { canvasErrorString, context } from "./main";
import {
  CollisionType,
  GhostConstructor,
  PositionType,
  VelocityType,
} from "./types";

export class Ghost {
  color: string;
  position: PositionType;
  prevCollisions: CollisionType[];
  radius: number;
  speed: number;
  velocity: VelocityType;
  static speed = 2;
  constructor({ color = "red", position, speed, velocity }: GhostConstructor) {
    this.position = position;
    this.velocity = velocity;
    this.radius = 15;
    // this.speed = 2;
    this.speed = speed;
    this.color = color;
    this.prevCollisions = [];
  }
  draw() {
    if (!context) {
      console.error(canvasErrorString);
      return;
    }
    context.beginPath();
    context.arc(this.position.x, this.position.y, this.radius, 0, Math.PI * 2);
    context.fillStyle = this.color;
    context.fill();
    context.closePath();
  }
  update() {
    this.draw();
    this.position.x += this.velocity.x;
    this.position.y += this.velocity.y;
  }
}