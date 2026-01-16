///<reference path="../../../typings/tsd.d.ts" />
///<reference path="../../../typings/typescriptApp.d.ts" />

class Grid {

    private space:String[];
    public width:number;
    public height:number;

    constructor(width, height) {
        this.space = new Array(width * height);
        this.width = width;
        this.height = height;
    }

    public isInside(vector:Vector) {
        return vector.x >= 0 && vector.x < this.width &&
            vector.y >= 0 && vector.y < this.height;
    }

    public get(vector:Vector) {
        return this.space[vector.x + this.width * vector.y];
    }

    public set(vector:Vector, value:String) {
        this.space[vector.x + this.width * vector.y] = value;
    }

    public forEach(f:any, context:any) {
        for (var y = 0; y < this.height; y++) {
            for (var x = 0; x < this.width; x++) {
                var value = this.space[x + y * this.width];
                if (value !== null) {
                    f.call(context, value, new Vector(x, y));
                }
            }
        }
    }

}