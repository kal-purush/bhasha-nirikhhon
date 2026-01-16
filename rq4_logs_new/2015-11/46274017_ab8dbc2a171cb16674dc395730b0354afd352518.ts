/// <reference path="TileModel.ts"/>
/// <reference path="PlayerModel.ts"/>

module Cm2k15 {
  export var directions = {
    Top: 'up',
    Right: 'right',
    Bottom: 'down',
    Left: 'left'
  };

  export class MapModel {
    public Tiles: TileModel[][];
    public Player: PlayerModel;

    public Width: number;
    public Height: number;

    constructor(width: number, height: number) {
      this.Width = width;
      this.Height = height;

      this.Tiles = [];
      for (var i = 0; i < height; i++) {
        this.Tiles[i] = [];
        for (var j = 0; j < width; j++) {
          this.Tiles[i][j] = new TileModel();
        }
      }

      this.Player = new PlayerModel();
      this.Player.X = 2;
      this.Player.Y = 2;

      this.Tiles[this.Player.X][this.Player.Y].Visited = true;
    }

    public MovePlayer(direction) {
      if ((this.Player.X == 0 && direction == directions.Left) ||
        (this.Player.Y == 0 && direction == directions.Top) ||
        (this.Player.X == this.Width - 1 && direction == directions.Right) ||
        (this.Player.Y == this.Height - 1 && direction == directions.Bottom)) {
        return "You can't leave this area, turn around!";
      } else {
        return this.Player.Move(direction);
      }
    }
  }
}