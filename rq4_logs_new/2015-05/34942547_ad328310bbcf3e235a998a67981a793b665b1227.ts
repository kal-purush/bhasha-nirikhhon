class Greeter {
  greeting: string;
  constructor(message: string) {
    this.greeting = message;
  }
  greet() {
    return 'Hello, ' + this.greeting;
  }
}

// Inheritance
class Animal {
  private name: string; // in TypeScript, each member is public by default
  constructor(name: string) {
    this.name = name;
  }
  move(meters: number = 0) {
    console.log(this.name + ' moved ' + meters + 'm.')
  }
}

class Snake extends Animal {
  constructor(name: string) {
    super(name);
  }
  move(meters = 5) {
    console.log("Slithering...");
    super.move(meters);
  }
}

class Horse extends Animal {
  constructor(name: string) {
    super(name);
  }
  move(meters = 45) {
    console.log('Galloping...');
    super.move(meters);
  }
}

var sam = new Snake('Sammy the Python');
var tom: Animal = new Horse('Tommy the Palomino');

sam.move();
tom.move(34);

// Accessors
var passCode = 'secret passcode';

class Employee {
  private _fullName: string;

  get fullName(): string {
    return this._fullName;
  }

  set fullName(newName: string) {
    if(passCode && passCode == 'secret passcode') {
      this._fullName = newName;
    }
    else {
      console.log('Error: Unauthorized update of employee!');
    }
  }
}

var employee = new Employee();
employee.fullName = "Bob Smith";
if (employee.fullName) {
  console.log(employee.fullName);
}

// Static Properties
class Grid {
  static origin = {x: 0, y: 0};
  calcuateDistanceFromOrigin(point: {x: number; y: number}) {
    var xDist = (point.x - Grid.origin.x);
    var yDist = (point.y - Grid.origin.y);
    return Math.sqrt(xDist * xDist + yDist * yDist) / this.scale;
  }
  constructor(public scale: number) { } // Parameter properties:  this.scale = scale
}

var grid1 = new Grid(1.0);

// Advanced Techniques
// Constructor functions
var greeter: Greeter;
greeter = new Greeter("world");
console.log(greeter.greet());

// Using a class as an interface
class Point {
  x: number;
  y: number;
}

interface Point3d extends Point {
  z: number;
}

var point3d: Point3d = {x: 1, y: 2, z: 3};