import Size = require("Size");
import Inputter = require("Inputter");
import Game = require("Game");
import Point = require("Point");
import IGameEntity = require("IGameEntity");

class Bullet implements IGameEntity {
    constructor(game: Game, position: Point) {
        this._game = game;
        this._position = position;
        this._size = this._game.size.resize(0.005);
        this._size.height = this._size.width;
    }

    private _game: Game;

    private _position: Point;

    private _size: Size;

    public draw(canvas: CanvasRenderingContext2D): void {
        canvas.fillStyle = "red";
        canvas.fillRect(this._position.x - this._size.width / 2, this._position.y - this._size.height / 2, this._size.width, this._size.height);
    }

    public update(): void {
        this._position.y--;

        if (this._position.y <= 0) {
            this._game.removeEntity(this);
        }
    }
}

export = Bullet;