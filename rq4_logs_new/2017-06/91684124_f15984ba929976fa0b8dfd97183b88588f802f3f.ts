/// <reference path="gameobject.ts"/>

class Coin extends GameObject {

    constructor(parent: HTMLElement, x: number, y: number) {
        super(parent, "coin", x, y, 15, 15);
    }

}