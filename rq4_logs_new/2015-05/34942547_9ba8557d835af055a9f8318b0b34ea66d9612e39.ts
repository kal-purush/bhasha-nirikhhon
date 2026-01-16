// Interfaces describe the public side of the class, rather than both the public and private side.
// This prohibits you from using them to check that a class also has particular types for the private side of the class instance.
interface LabelledValue {
  label: string
}

function printLabel (labelledObj: LabelledValue) {
  console.log(labelledObj.label);
}

var myObj = { size: 10, label: 'Size 10 Object' };
printLabel(myObj);

// Optional properties
interface SquareConfig {
  color?: string;
  width?: number;
}

function createSquare (config: SquareConfig): { color: string; area: number} {
  var newSquare = { color: 'white', area: 100};
  if (config.color) {
      newSquare.color = config.color;
  }
  if (config.width) {
      newSquare.area = config.width * config.width;
  }
  return newSquare;
}

var mySquare = createSquare({color: "black"});

// Function Types
interface SearchFunc {
  (source: string, subString: string): boolean;
}

var mySearch = function (source: string, subString: string) {
  var result = source.search(subString);
  if (result == -1) {
      return false;
  }
  else {
      return true;
  }
}

// Array Types
interface  StringArray {
  [index: number]: string; // There are two types of supported index types: string and number.
}

var myArray: StringArray;
myArray = ['Bob', 'Fred']

// Class Types
interface ClockInterface {
  currentTime: Date;
  setTime(d: Date);
}

class Clock implements ClockInterface {
  currentTime: Date;
  setTime(d: Date) {
    this.currentTime = d;
  }
  constructor (h: number, m: number) {}
}

// Extending Interfaces
interface Shape {
  color: string;
}

interface Square extends Shape {
  sideLength: number;
}

var square = <Square>{};
square.color = "blue";
square.sideLength = 10;

interface Shape2 {
  color: string;
}

interface PenStroke {
  penWidth: number;
}

interface Square2 extends Shape2, PenStroke {
  sideLength: number;
}

var square2 = <Square2>{};
square2.color = "blue";
square2.sideLength = 10;
square2.penWidth = 5.0;

// Hybrid Types
interface Counter {
  interval: number;
  reset(): void;
}

class SimpleCounter implements  Counter {
  interval: number;
  reset(): void {
    this.interval = null;
  }
  constructor (start: number) {
    this.interval = start;
  }
}

var simpleCounter = new SimpleCounter(10);