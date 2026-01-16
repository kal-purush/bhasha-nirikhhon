/// <reference path="babylon.2.2.d.ts"/>
/// <reference path="Game.ts"/>

document.addEventListener("DOMContentLoaded", function () {
    var myGame;

    if (BABYLON.Engine.isSupported()) {
        myGame = new GAME(2);
    }
}, false);