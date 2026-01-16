module DrawingTs {
    'use strict'
   
    export class Point {
        constructor(public x: number, public y: number) { }

        public to(p: Point): Vector {
            return new Vector(p.x - this.x, p.y - this.y);
        }

        public scale(n: number)
        {
            return new Point(this.x * n, this.y * n);
        }
    }
    
    export class Vector extends Point {
        constructor(x: number, y: number) {
            super(x, y);
        }

        public norm(): number {
            return Math.sqrt(this.x * this.x + this.y * this.y);
        }

        public add(v: Vector): Vector {
            return new Vector(this.x + v.x, this.y + v.y);
        }

        public subtract(v: Vector): Vector {
            return new Vector(this.x - v.x, this.y - v.y);
        }

        public multiple(n: number): Vector {
            return new Vector(n * this.x, n * this.y);
        }

        public dotProduct(v: Vector): number {
            return this.x * v.x + this.y * v.y;
        }

        public crossProduct(v: Vector): Vector {
            return new Vector(this.x * v.y - v.x * this.y, this.y * v.x - v.y * this.x);
        }
    }
    
    export class Rectangle {
        constructor(public x: number, public y: number,
            public w: number, public h: number) { }

        public get left(): number { return this.x; }
        public get right(): number { return this.x + this.w; }
        public get top(): number { return this.y; }
        public get bottom(): number { return this.y + this.h }

    }

}