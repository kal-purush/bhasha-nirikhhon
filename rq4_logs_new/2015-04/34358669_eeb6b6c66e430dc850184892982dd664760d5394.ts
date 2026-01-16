/// <reference path='Observable.ts' />
class Circle extends Observable {

    private r:number;

    get R():number {
        return this.r;
    }

    set R(val:number) {
        if (this.r !== val) {
            this.r = val;
            this.propertyChanged('R');
        }
    }

    get Area():number {
        return Math.pow(this.R, 2) * Math.PI;
    }

    constructor(r:number = 0) {
        super();
        this.R = r;
    }
}