class Rect {
	private t: number;
	private l: number;
	private r: number;
	private b: number;
	public get top() { return this.t; }
	public get left() { return this.l; }
	public get right() { return this.r; }
	public get bottom() { return this.b; }
	public get width() { return this.right - this.left + 1; }
	public get height() { return this.bottom - this.top + 1; }

	constructor(top: number, left: number, right: number, bottom: number) {
		this.t = top; this.l = left; this.r = right; this.b = bottom;
	}

	public static around(o: Offset): Rect {
		return new Rect(o.y-1, o.x-1, o.x+1, o.y+1);
	}

	public contains(o: Offset){
		return o.x>=this.left && o.x<=this.right
			&& o.y>=this.top && o.y<= this.bottom;
	}
	public enum(visitor: (o: Offset, i: number)=>void) {
		var i = 0;
		for(var y = this.top; y<=this.bottom; y++)
			for(var x = this.left; x<=this.right; x++)
				visitor(new Offset(x, y), i++);
	}
	public indexOf(offset: Offset): number {
		return (offset.y - this.top) * this.width + offset.x - this.left;
	}

	// Returns a rect that has the outer-rim of this one removed.
	public trimBorder(): Rect;
	public trimBorder(amount: number): Rect;
	public trimBorder(tx: number, ty: number): Rect;
	public trimBorder(tl: number, tr: number, tt: number, tb: number): Rect;
	public trimBorder(...args: number[]): Rect {
		if(args.length < 2) {
			var t = args.length===1 ? args[0] : 1;
			return new Rect(this.top+t, this.left+t, this.right-t, this.bottom-t);
		}

		if(args.length===2)
			return new Rect(this.top+args[1], this.left+args[0], this.right-args[0], this.bottom-args[1]);

		if(args.length===4)
			return new Rect(this.top+args[2], this.left+args[0], this.right-args[1], this.bottom-args[3]);

		throw 'Rect.trimBorder should have 0, 1, 2, or 4 arguments.';
	}
}

class Offset {
	private _x: number;
	private _y: number;
	public get x(){return this._x;}
	public get y(){return this._y;}
	constructor(x:number, y:number){this._x = x; this._y = y;}
	public is(x:number,y:number){
		return this.x===x && this.y===y;
	}
	public delta(dx: number, dy: number) { return new Offset(this.x+dx, this.y+dy); }
	public toString() { return `(${this.x},${this.y})`; }
	public equals(other: Offset) { return this.x == other.x && this.y == other.y; }
}