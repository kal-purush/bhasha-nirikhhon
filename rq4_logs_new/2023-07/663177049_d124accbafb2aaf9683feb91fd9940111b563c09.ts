import { BoardVector2d } from "geometry";

enum PieceModel{
  KING    = 0,
  QUEEN   = 1,
  PAWN    = 2,
  BISHOP  = 3,
  ROOK    = 4,
  KNIGHT  = 5,
  MIRROR  = 6,
  LASGUN  = 7
}

class Piece {
  private readonly model: PieceModel;
  private readonly initialPosition: BoardVector2d;
  private position: BoardVector2d;
  private readonly playerId: number;
  private moveCounter: number;
  private direction: BoardVector2d | undefined;

  public constructor(model: PieceModel, position: BoardVector2d, playerId: number, direction: number){
    this.model = model;
    this.initialPosition = position;
    this.position = position;
    this.moveCounter = 0;
    this.direction = this.direction; 
  }
}