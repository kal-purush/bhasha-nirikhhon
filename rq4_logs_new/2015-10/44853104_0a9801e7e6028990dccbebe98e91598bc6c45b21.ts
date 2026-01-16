import { Chess } from './glossary';
import { Square } from './square';

export class Piece {
  constructor(public side: Chess.Side, public square: Square) {}

  toString() {
    return 'Piece';
  }

  toJson() {
    return this.toString();
  }
}

export class King extends Piece {
  toString() {
    return 'King';
  }
}
export class Queen extends Piece {
  toString() {
    return 'Queen';
  }
}
export class Rook extends Piece {
  toString() {
    return 'Rook';
  }
}
export class Bishop extends Piece {
  toString() {
    return 'Bishop';
  }
}
export class Knight extends Piece {
  toString() {
    return 'Knight';
  }
}
export class Pawn extends Piece {
  toString() {
    return 'Pawn';
  }
}