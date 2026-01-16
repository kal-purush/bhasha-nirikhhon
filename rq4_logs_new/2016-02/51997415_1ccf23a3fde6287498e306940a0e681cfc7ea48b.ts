'use strict';

export class Grid {

    private _height:number;
    private _width:number;
    private _values:Array<number>;

    constructor(gridWidth:number, gridHeight:number, initial:Array<number>) {
        this._height = gridHeight;
        this._width = gridWidth;
        this._values = initial;
    }

    get width():number {
        return this._width;
    }

    get height():number {
        return this._height;
    }

    set(first:number, second:number, third?:number) {
        if (third === undefined) {
            this._setViaOffset(first, second);
        } else {
            this._setViaXY(first, second, third);
        }
    }

    get(first:number, second?:number) {
        if (second === undefined) {
            return this._getViaOffset(first);
        }
        return this._getViaXY(first, second);
    }

    private _setViaXY(x:number, y:number, value:number):void {
        this._setViaOffset(this._calculateOffset(x, y), value);
    }

    private _setViaOffset(offset:number, value:number):void {
        this._values[offset] = value;
    }

    private _getViaXY(x:number, y:number):number {
        return this._getViaOffset(this._calculateOffset(x, y));
    }

    private _getViaOffset(offset:number):number {
        return this._values[offset];
    }

    private _calculateOffset(x:number, y:number):number {
        return x + y * this.width;
    }
}