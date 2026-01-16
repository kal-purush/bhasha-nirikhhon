"use strict";

function buttonFunc() {
    printNum(dice.roll());
}

function printNum(num) {
    document
        .getElementById('diceBox')
        .innerHTML = num;
}

// Trying out an object with paremeters and methods
var dice = {
    sides: 6,
    roll: function () {
        return Math.floor(Math.random() * this.sides) + 1;
    }
};