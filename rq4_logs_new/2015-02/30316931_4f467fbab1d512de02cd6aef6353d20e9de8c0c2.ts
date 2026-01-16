import Size = require("Size");
import Point = require("Point");
import Player = require("Player");
import IGameEntity = require("IGameEntity");

class Game {
    constructor(public element: HTMLCanvasElement) {
        this._size = new Size(element.width, element.height);

        this._entities.push(new Player(new Point(this._size.width / 2, this._size.height - 50)));

        this._drawingContext = this.element.getContext("2d");
    }

    private _drawingContext: CanvasRenderingContext2D;

    private _size: Size;

    private _entities: IGameEntity[] = [];

    public begin(): void {
        this._startTick(() => {
            this.update();
            this.draw();
        });
    }

    public update() {
    }

    public draw() {
        this._entities.forEach(entity => {
            entity.draw(this._drawingContext);
        });
    }

    private _startTick(fn) {
        var tick = () => {
            fn();
            requestAnimationFrame(tick);
        };

        requestAnimationFrame(tick);
    }
}

export = Game;