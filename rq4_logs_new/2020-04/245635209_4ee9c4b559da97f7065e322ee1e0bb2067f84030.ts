// Examples of Generic Classes
class ArrayOfNumber {
  constructor(public collection: number[]) {}

  get(index: number): number {
    return this.collection[index];
  }
}

class ArrayOfString {
  constructor(public collection: string[]) {}

  get(index: number): string {
    return this.collection[index];
  }
}

class ArrayOfAnything<T> {
  constructor(public collection: T[]) {}

  get(index: number): T {
    return this.collection[index];
  }
}

const arrStrAnnotation = new ArrayOfAnything<string>(['a', 'b', ' c']);
const arrStrInference = new ArrayOfAnything(['aa', 'bb', ' cc']);

// Examples of Generic functions
function printNumbers(arr: number[]): void {
  for (let i = 0; i < arr.length; i++) {
    console.log(arr[i]);
  }
}

function printStrings(arr: string[]): void {
  for (let i = 0; i < arr.length; i++) {
    console.log(arr[i]);
  }
}

function printAnythings<T>(arr: T[]): void {
  for (let i = 0; i < arr.length; i++) {
    console.log(arr[i]);
  }
}

printAnythings<string>(['a', 'b', 'f']);
printAnythings([1, 2, 3, 45, 5]);

// Generic constrains
class Automobile {
  print(): void {
    console.log('I am an Automobile');
  }
}

class House {
  print(): void {
    console.log('I am an House');
  }
}

interface Printable {
  print(): void;
}

function printAutomobilesOrHouses<T extends Printable>(arr: T[]): void {
  for (let i = 0; i < arr.length; i++) {
    arr[i].print();
  }
}

printAutomobilesOrHouses<Automobile>([new Automobile(), new Automobile()]);
printAutomobilesOrHouses<House>([new House(), new House()]);