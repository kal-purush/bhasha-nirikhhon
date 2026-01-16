/// <reference path="typings/easeljs/easeljs.d.ts" />
/// <reference path="typings/tweenjs/tweenjs.d.ts" />
/// <reference path="typings/soundjs/soundjs.d.ts" />
/// <reference path="typings/preloadjs/preloadjs.d.ts" />

// Game Framework Variables
var canvas = document.getElementById("canvas");
var stage: createjs.Stage;

//Game Variables
var helloLabel: createjs.Text; //create a reference

function init() {
    stage = new createjs.Stage(canvas);
    stage.enableMouseOver(20); //count the number of times the mouse hover over the buttons
    createjs.Ticker.setFPS(60); // framerate for the game
    createjs.Ticker.on("tick", gameLoop);

    main();

}

//Our Main Game Loop refreshed - 60 fps
function gameLoop() {
    stage.update();
}

//Our Main Game Function
function main() {
    console.log("Game is Running");
    helloLabel = new createjs.Text("Hello World!", "40px Consolas", "#000000");

    helloLabel.regX = helloLabel.getMeasuredWidth() * 0.5;
    helloLabel.regY = helloLabel.getMeasuredHeight() * 0.5;
    helloLabel.x = 160;
    helloLabel.y = 220;

    stage.addChild(helloLabel);


}

