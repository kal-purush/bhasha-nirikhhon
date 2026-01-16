module Cm2k15 {
    export class GameMapState {
        public Tiles: TileState[][];
        public Player: PlayerState;

        public Width: number;
        public Height: number;

        constructor(width:number, height:number) {
            this.Width = width;
            this.Height = height;

            this.Tiles = [];
            for (var i = 0; i < height; i++) {
                this.Tiles[i] = [];
                for (var j = 0; j < width; j++) {
                    this.Tiles[i][j] = new TileState();
                }
            }

            this.Player = new PlayerState();
            this.Player.X = 2;
            this.Player.Y = 2;

            this.Tiles[this.Player.X][this.Player.Y].Visited = true;
        }
    }

    class TileState {
        public Visited: boolean;

        constructor() {
            this.Visited = false;
        }
    }

    class PlayerState {
        public X: number;
        public Y: number;

        public Move(direction: string) {
            switch (direction) {
                case 'up':
                    this.Y--;
                    break;
                case 'down':
                    this.Y++;
                    break;
                case 'left':
                    this.X--;
                    break;
                case 'right':
                    this.X++;
                    break;
                default:
                    return 'Move where?';
            }

            return "You have moved " + direction + ".";
        }
    }
}