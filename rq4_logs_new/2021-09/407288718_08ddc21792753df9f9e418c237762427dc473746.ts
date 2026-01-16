export class Point {
    x:number;
    y:number;

    constructor(x: number, y: number) {
        this.x = x;
        this.y = y;
    }

    toString(): string {
        return `(${this.x}, ${this.y})`
    }

    private _calcDistance(xCoords: [number, number], yCoords: [number, number]) {
        return Math.sqrt(Math.pow(xCoords[0] - xCoords[1], 2) + Math.pow(yCoords[0] - yCoords[1], 2))
    }

    distance(nextX?: Point | number, nextY?: number) {
        if(!nextX) {
            return Math.abs(this._calcDistance([this.x, 0], [this.y, 0]))
        }

        if(nextX instanceof Point) {
            const {x: x2, y: y2} = nextX;

            return this._calcDistance([this.x, x2], [this.y, y2])
        }
        
        if(typeof nextX === 'number' && typeof nextY === 'number') {
            return this._calcDistance([this.x, nextX], [this.y, nextY])
        }

    }
}