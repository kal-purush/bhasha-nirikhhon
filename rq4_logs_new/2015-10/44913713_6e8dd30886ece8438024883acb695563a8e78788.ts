// There are 3 basic types in TypeScript
var name: string = "Jonn";
var isSuccess: boolean = false;
var age: number = 35;
var foo: any = "a string"; // Use 'any' type when it's impossible to know
foo = false;
foo = 9";

// For collections, there are typed arrays and generic arrays
var list: number[] = [1, 2, 3];
var list: Array<number> = [1, 2, 3]; //generic array type

// For enumerations:
enum Fruit {Apple, Avocado, Grapes};
var foo: Fruit = Fruit.Avocado;

// "void" is used when a function returning nothing
function sayWada(): void {
  console.log('wada!');
}

// More function samples
var foo1 = function(i: number): number { return i * i; }
var foo2 = function(i: number) { return i * i; }
var foo3 = (i: number): number => { return i * i; }
var foo4 = (i: number) => { return i * i; }
var foo5 = (i: number) =>  i * i;

// Interfaces are structural, anything that has the properties is compliant with the interface
interface Person {
  name: string;
  // Optional properties, marked with a "?"
  age?: number;
  // And of course functions
  move(): void;
}

// Object that implements the "Person" interface
// Can be treated as a Person since it has the name and move properties
var p: Person = { name: "Bobby", move: () => {} };
// Objects that have the optional property:
var validPerson: Person = { name: "Bobby", age: 42, move: () => {} };
// Is not a person because age is not a number
var invalidPerson: Person = { name: "Bobby", age: true };

// Interfaces can also describe a function type
interface SearchFunc {
  (source: string, subString: string): boolean;
}
// Only the parameters' types are important, names are not important.
var mySearch: SearchFunc;
mySearch = function(src: string, sub: string) {
  return src.search(sub) != -1;
}

// Classes - members are public by default
class Point {
  // Properties
    x: number;

    // Constructor - the public/private keywords in this context will generate
    // the boiler plate code for the property and the initialization in the
    // constructor.
    // In this example, "y" will be defined just like "x" is, but with less code
    // Default values are also supported

    constructor(x: number, public y: number = 0) {
        this.x = x;
    }

    // Functions
    dist() { return Math.sqrt(this.x * this.x + this.y * this.y); }

    // Static members
    static origin = new Point(0, 0);
}

var p1 = new Point(10 ,20);
var p2 = new Point(25); //y will be 0

// Inheritance
class Point3D extends Point {
    constructor(x: number, y: number, public z: number = 0) {
        super(x, y); // Explicit call to the super class constructor is mandatory
    }

    // Overwrite
    dist() {
        var d = super.dist();
        return Math.sqrt(d * d + this.z * this.z);
    }
}

// Modules, "." can be used as separator for sub modules
module Geometry {
  export class Square {
    constructor(public sideLength: number = 0) {
    }
    area() {
      return Math.pow(this.sideLength, 2);
    }
  }
}

var s1 = new Geometry.Square(5);

// Local alias for referencing a module
import G = Geometry;

var s2 = new G.Square(10);

// Generics
// Classes
class Tuple<T1, T2> {
    constructor(public item1: T1, public item2: T2) {
    }
}

// Interfaces
interface Pair<T> {
    item1: T;
    item2: T;
}

// And functions
var pairToTuple = function<T>(p: Pair<T>) {
    return new Tuple(p.item1, p.item2);
};

var tuple = pairToTuple({ item1:"hello", item2:"world"});

// Including references to a definition file:
/// <reference path="jquery.d.ts" />