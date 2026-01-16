"use strict";

class Room {
    constructor (name, width, length) {
        this.name = name
        this.width = width
        this.length = length
    }

    get area() {
        return this.width * this.length
    }
}

const lobby = new Room ('lobby', 3, 4)

console.log (lobby);

