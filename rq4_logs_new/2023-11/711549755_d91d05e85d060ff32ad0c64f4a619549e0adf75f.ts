// Створіть класи Circle, Rectangle, Square і Triangle.
// У кожного з них є загальнодоступний метод calculateArea.
// У кожної фігури є загальнодоступні властивості - колір і назва, які не можна змінювати після створення.
// У Square і Rectangle зі свого боку є ще додатковий метод print, який виводить рядок із формулою розрахунку площі

abstract class Shape {
  constructor(public readonly name: string, public readonly color: string) {}

  abstract calculateArea(): number;
}

class Circle extends Shape {
  constructor(readonly name: string, readonly color: string, public readonly radius: number) {
    super(name, color);
  }

  calculateArea(): number {
    return Number((Math.PI * this.radius ** 2).toFixed(2));
  }
}

class Triangle extends Shape {
  constructor(
    readonly name: string,
    readonly color: string,
    public readonly base: number,
    public readonly height: number,
  ) {
    super(name, color);
  }

  calculateArea(): number {
    return Number((0.5 * this.base * this.height).toFixed(2));
  }
}

class Rectangle extends Shape {
  constructor(
    readonly name: string,
    readonly color: string,
    public readonly width: number,
    public readonly height: number,
  ) {
    super(name, color);
  }

  calculateArea(): number {
    return Number((this.width * this.height).toFixed(2));
  }

  print(): void {
    console.log(`Rectangle Formula: Area = ${this.width} * ${this.height}`);
  }
}

class Square extends Rectangle {
  constructor(readonly name: string, readonly color: string, public readonly sideLength: number) {
    super(name, color, sideLength, sideLength);
  }

  print(): void {
    console.log(`Square Formula: Area = ${this.sideLength} * ${this.sideLength}`);
  }
}

const circle = new Circle('Circle', 'magenta', 8);
console.log(circle.calculateArea()); // 201.06

const triangle = new Triangle('Triangle', 'purple', 3, 9);
console.log(triangle.calculateArea()); //13.5

const rectangle = new Rectangle('Rectangle', 'brown', 5, 8);
console.log(rectangle.calculateArea()); // 40
rectangle.print(); //Rectangle Formula: Area = 5 * 8

const square = new Square('Square', 'green', 7);
console.log(square.calculateArea()); //49
square.print(); //Square Formula: Area = 7 * 7