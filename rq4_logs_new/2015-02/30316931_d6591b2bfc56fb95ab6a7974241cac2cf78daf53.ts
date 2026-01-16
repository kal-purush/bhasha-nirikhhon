import Size = require("Size");
import Bullet = require("Bullet");
import Inputter = require("Inputter");
import Game = require("Game");
import Point = require("Point");
import IGameEntity = require("IGameEntity");

class Invader implements IGameEntity {
    constructor(game: Game, position: Point) {
        this._game = game;
        this.position = position;
        this.size = this._game.size.resize(0.025);
        this.size.height = this.size.width;
        this._direction = 1;
        this._patrolX = 0;
        this._speed = this._game.size.width * 0.0025;
    }

    public position: Point;

    public size: Size;

    private _game: Game;

    private _direction: number;

    private _patrolX: number;

    private _speed: number;

    public draw(canvas: CanvasRenderingContext2D): void {
        canvas.fillStyle = "#aaa";
        canvas.fillRect(this.position.x - this.size.width / 2, this.position.y - this.size.height / 2, this.size.width, this.size.height);
    }

    public update(): void {
        if (this._patrolX > this._game.size.width / 2.75 || this._patrolX < 0) {
            this._direction *= -1;
            this.position.y += this.size.height * 1.25;
        }

        this.position.x += this._speed * this._direction;
        this._patrolX += this._speed * this._direction;
    }
}

export = Invader;