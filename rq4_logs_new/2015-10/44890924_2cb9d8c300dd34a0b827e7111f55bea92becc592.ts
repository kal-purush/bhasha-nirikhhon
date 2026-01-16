///<reference path="../../../typings/tsd.d.ts" />
///<reference path="../../../typings/typescriptApp.d.ts" />

class Bullet extends Critter {

    private orientation:string;
    public originChar = 'b';

    constructor(direction) {
        super(direction);
        this.orientation = this.direction;
    }

    public act(vector:Vector) {
        var view = new View(vector);
        if (view.look(this.direction) !== ' ') {
            this.explode(vector);
        } else {
            this.move(vector);
        }
    }

    private move(vector) {
        var dest = world.checkDestination(this.direction, vector);
        world.grid.set(vector, null);
        world.grid.set(dest, this);
    }

    private explode(vector) {
        var dest = world.checkDestination(this.direction, vector);

        //kill shot
        if (world.grid.get(dest).energy !== undefined) {
            console.log('kill shot');
            world.grid.set(dest, null);
        };

        world.grid.set(vector, null);
    }

}