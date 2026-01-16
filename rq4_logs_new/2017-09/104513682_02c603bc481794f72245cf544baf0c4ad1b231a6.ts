import Entity from "./Entity"
import { Pos } from "./Interfaces"

export default class Box extends Entity {
    constructor(private _x:number = 0,
                private _y:number = 0,
                private _width:number,
                private _height:number,
                private _color:string = '#aaa')
    {
        super();
    }

    pos(): Pos {
        return {x: this._x, y: this._y};
    }
    
    setPos(x: number, y: number) {
        this._x = x;
        this._y = y;
    }

    size() {
        return { width: this._width, height: this._height };
    }

    setSize(width: number, height: number) {
        this._width = width;
        this._height = height;
    }

    color(): string {
        return this._color;
    }

    setColor(color: string) {
        this._color = color;
    }
    
    update() {
        /*this.components.forEach(element => {
            element.update();
        });*/
    }

    draw() {
        Globais.ctx.fillStyle = this._color;
        Globais.ctx.fillRect(this._x, this._y, this._width, this._height);
    }
}