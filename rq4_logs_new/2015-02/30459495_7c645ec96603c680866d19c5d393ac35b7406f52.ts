var canvas;
var stage: createjs.Stage;

// Game Objects 
var game: createjs.Container;
var background: createjs.Bitmap;

function init() {
    canvas = document.getElementById("canvas");
    stage = new createjs.Stage(canvas);
    stage.enableMouseOver(20); // Enable mouse events
    createjs.Ticker.setFPS(60); // 60 frames per second
    createjs.Ticker.addEventListener("tick", gameLoop);

    main();
}

function gameLoop() {
    stage.update(); // Refreshes our stage
}

// Event handlers


//function to instantiate all game controls for UI
function createUI() {
    //instantiate my background
    background = new createjs.Bitmap("assets/images/background.png");
    game.addChild(background);
}

// Our Game Kicks off in here
function main() {
    //Instantiate game container
    game = new createjs.Container();
    //Create User interface
    createUI();
    //Add the game container to the stage
    stage.addChild(game);
}