enum Pepper {
  Red,
  Blue,
  Yellow,
  Green,
  Orange,
  Purple,
  Brown,
  White,
  Black,
  Ghost
}

enum Player {
  Red,
  Blue,
  Yellow,
  Green,
  Orange,
  Purple
}

class Point {
  x: number;
  y: number;

  constructor(x: number, y: number) {
    this.x = x;
    this.y = y;
  }
}

class PepperPatch {
  // TODO: use fp-ts Optional?
  peppers: Pepper|null[];
  farmers: Player|null[];


  constructor() {
    this.peppers = [];
    this.farmers = [];
  }
}

class ScovilleGame {
  players: Player[];
  patch: PepperPatch;
}