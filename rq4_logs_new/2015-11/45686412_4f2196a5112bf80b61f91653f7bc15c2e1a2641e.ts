class Grid {
    static origin = {x: 0, y: 0};
    calculateDistanceFromOrigin(point: {x: number; y: number;}) {
        var xDist = (point.x - Grid.origin.x);
        var yDist = (point.y - Grid.origin.y);
        return Math.sqrt(xDist * xDist + yDist * yDist) / this.scale;
    }
    constructor (public scale: number) { }
}

var grid1 = new Grid(1.0);  // 1x scale
var grid2 = new Grid(5.0);  // 5x scale
var grid3 = new Grid(3.0);
var grid4 = new Grid(2.0);

console.log(grid1.calculateDistanceFromOrigin({x: 10, y: 10}));
console.log(grid2.calculateDistanceFromOrigin({x: 10, y: 10}));
console.log(grid2.calculateDistanceFromOrigin({num1: 20, num2: 10}));

Grid.origin = {x:2,y:2};
console.log(grid2.calculateDistanceFromOrigin(Grid.origin));
console.log(grid2.calculateDistanceFromOrigin(Grid.origin));

class Properties
{
    add(static x:number , static y:number)  //Error Static modifer cannot appear in parameters.
    {
        return num1+num2;
    }
}