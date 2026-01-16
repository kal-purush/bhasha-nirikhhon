import { Integer, int } from "utils";

export enum Symmetry {
  ORIGIN = 0,
  X_AXIS = 1,
  Y_AXIS = 2
}


export class Vector2d {
  protected x: number;
  protected y: number;

  protected createVector(x: number, y: number): Vector2d {
    return new Vector2d(x, y);
  }

  public constructor(x: number, y: number) {
    this.x = x;
    this.y = y;
  }

  public getX() {
    return this.x;
  }

  public getY() {
    return this.y;
  }

  public toString() {
    return this.x + ", " + this.y;
  }

  public equals(other: unknown): boolean {
    if (!(other instanceof Vector2d)) {
      return false;
    }
    return this.x == other.x && this.y == other.y;
  }

  public add(other: Vector2d): Vector2d {
    return this.createVector(this.x + other.x, this.y + other.y);
  }

  public sub(other: Vector2d): Vector2d {
    return this.createVector(this.x - other.x, this.y - other.y);
  }

  public mul(other: Vector2d | number): Vector2d {
    if (other instanceof Vector2d) {
      return this.createVector(this.x * other.x, this.y * other.y);
    }
    return this.createVector(this.x * other, this.y * other);
  }

  public div(other: Vector2d | number): Vector2d {
    if (other instanceof Vector2d) {
      return this.createVector(this.x / other.x, this.y / other.y);
    }
    return this.createVector(this.x / other, this.y / other);
  }

  public opposite(): Vector2d {
    return this.createVector(-this.x, -this.y);
  }

  public reverseAxis(): Vector2d {
    return this.createVector(this.y, this.x);
  }

  public copy(): Vector2d {
    return new Vector2d(this.x, this.y)
  }

  public pivotSymmetry(symmetry: Symmetry): Vector2d {
    switch (symmetry) {
      case Symmetry.ORIGIN:
        return this.opposite();
      case Symmetry.X_AXIS:
        return this.createVector(this.x, -this.y);

      case Symmetry.Y_AXIS:
        return this.createVector(-this.x, this.y);
    }
  }
}

export class IntVector2d extends Vector2d {
  public constructor(x: int, y: int) {
    super(Integer.roundToInt(x), Integer.roundToInt(y))
  }

  protected createVector(x: int, y: int): Vector2d {
    return new IntVector2d(x, y);
  }

  public copy(): IntVector2d {
    return new IntVector2d(this.x as int, this.y as int)
  }


  public equals(other: unknown): boolean {
    if (!(other instanceof IntVector2d)) {
      return false;
    }
    return this.x == other.x && this.y == other.y;
  }
}

export class BoardVector2d extends IntVector2d {

  public static fromVector2d(vector: Vector2d | IntVector2d): BoardVector2d {
    return new BoardVector2d(vector.getX() as int, vector.getY() as int);
  }

  public static fromString(vectorString: string): BoardVector2d {
    if (vectorString.length != 2) {
      throw new Error("Vector string is invalid [fromString()]");
    }
    return new BoardVector2d((vectorString.charCodeAt(0) - 97) as int, (vectorString.charCodeAt(1) - 1) as int);
  }

  public xToString(): string {
    return String.fromCharCode(this.x + 97);
  }

  public yToString(): string {
    return (this.y + 1).toString();
  }

  public toString(): string {
    return this.xToString() + this.yToString();
  }

  public equals(other: unknown): boolean {
    if (!(other instanceof BoardVector2d)) {
      return false;
    }
    return this.x == other.x && this.y == other.y;
  }

  public copy(): BoardVector2d {
    return new BoardVector2d(this.x as int, this.y as int)
  }
}