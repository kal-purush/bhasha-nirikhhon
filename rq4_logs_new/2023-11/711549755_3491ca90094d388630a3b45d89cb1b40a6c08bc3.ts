import { IPrintable } from "../interfaces";
import { IRectangle } from "../interfaces/rectangle";
import { Shape } from "./shape";

export class Rectangle extends Shape implements IRectangle, IPrintable {
  constructor(
    color: string,
    name: string,
    readonly width: number,
    readonly height?: number
  ) {
    super(color, name);
  }

  calculateArea(): number {
    return this.width * (this.height ?? this.width);
  }

  print(): void {
    console.log("width * height");
  }
}

class Bird {
  readonly type = "Bird";
}

class Fish {
  readonly type = "Fish";
}

function foo(animal: Bird | Fish): animal is Bird {
  return animal.type === "Bird";
}