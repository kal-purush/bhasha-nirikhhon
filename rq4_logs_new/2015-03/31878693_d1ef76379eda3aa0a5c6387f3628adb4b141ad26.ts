// CreateJS Boilerplate for COMP397


// VARIABLES ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
var canvas; // Reference to the HTML 5 Canvas element
var stage: createjs.Stage; // Reference to the Stage
//var helloText: createjs.Text; // Holds my Hello World! Text
//var buttonBitmap: createjs.Bitmap;
var assets: createjs.LoadQueue;
// FUNCTIONS ++++++++++++++++++++++++++++++++++++++++++++++++++++++++

function preload() {
    assets = new createjs.LoadQueue();
    assets.installPlugin(createjs.Sound);
    assets.on("complete", init, this);
    //queue.loadFile({ id: "sound", src: "" });
    assets.loadManifest([
        { id: "allien", src: "assets/images/allien2.png" },
        { id: "asteroid", src: "assets/images/asteroid.png" }

    ]);

}

function init() {


    //asas
    canvas = document.getElementById("canvas");
    stage = new createjs.Stage(canvas); // Parent Object
    stage.enableMouseOver(20); // Turn on Mouse Over events

    createjs.Ticker.setFPS(60); // Set the frame rate to 60 fps
    createjs.Ticker.addEventListener("tick", gameLoop);

    main();
}


// GAMELOOP
function gameLoop() {
    stage.update();
}

/*
function buttonClicked() {
    //helloText.text = "Goodbye!";
}

function buttonOut() {
    //buttonBitmap.alpha = 1;
}

function buttonOver() {
    //buttonBitmap.alpha = 0.5;
}
*/

function main() {
   /* // This is where all the work happens
    helloText = new createjs.Text("Hello World!", "40px Consolas", "#000000");
    stage.addChild(helloText); // First Child Object that we add to the stage

    // Green Button
    buttonBitmap = new createjs.Bitmap("assets/images/button-small.png");
    buttonBitmap.x = 100;
    buttonBitmap.y = 100;
    buttonBitmap.addEventListener("click", buttonClicked);
    buttonBitmap.addEventListener("mouseout", buttonOut);
    buttonBitmap.addEventListener("mouseover", buttonOver);

    stage.addChild(buttonBitmap);
*/


}


