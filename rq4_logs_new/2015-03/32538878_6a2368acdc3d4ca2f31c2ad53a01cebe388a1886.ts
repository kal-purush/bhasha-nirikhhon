/// <reference path="typings/createjs-lib/createjs-lib.d.ts" />
/// <reference path="typings/easeljs/easeljs.d.ts" />
/// <reference path="typings/tweenjs/tweenjs.d.ts" />
/// <reference path="typings/soundjs/soundjs.d.ts" />
/// <reference path="typings/preloadjs/preloadjs.d.ts" />


/// <reference path="objects/barry.ts" />
/// <reference path="objects/missles.ts" />
/// <reference path="objects/background.ts" />
/// <reference path="objects/coins.ts" />




/**
File Name: game.ts
Author: Chandan Dadral
Purpose: This file contains initialization, preload, and state machine for the
arcade game
*/

var stage: createjs.Stage;
var game: createjs.Container;
var queue;



// Game Objects
var barry: objects.Barry;
var coins: objects.Coins;
var background: objects.Background;
var scoreboard: objects.scoreBoard;


// Missles Array
var missles = [];



// State variables
var currentState: number;
var currentStateFunction;

// Pre-load function - this loads all of the assets ahead of time
function preload(): void {
    queue = new createjs.LoadQueue();
    // Load the sound plugin
    queue.installPlugin(createjs.Sound);
    createjs.Sound.alternateExtensions = ["mp3"];

    queue.addEventListener("complete", init);
    queue.loadManifest([
        { id: "backAudio", src: "assets/audio/backsound.mp3" },
        { id: "coinCollect", src: "assets/audio/coin_collect.mp3" },
        { id: "lifeUpAudio", src: "assets/audio/lifeUp.mp3" },
        { id: "explosionAudio", src: "assets/audio/Explosion.mp3" },
        { id: "mainMenu", src: "assets/audio/back.mp3" },
        { id: "hover", src: "assets/audio/hover.mp3" },
        
        { id: "barry", src: "assets/img/game_char.png" },
        { id: "background", src: "assets/img/background.png" },
        { id: "bullet", src: "assets/img/bullet-basic.png" },
        { id: "missles", src: "assets/img/missles.png" },
        { id: "coins", src: "assets/img/StarCoin.png" },
        { id: "gameLogo", src: "assets/img/Logo.png" },
       
    ]);
}

// Initialization function - This is where the stage gets created, everything gets set up
function init(): void {
    stage = new createjs.Stage(document.getElementById("gameCanvas"));
    stage.enableMouseOver(20);
    createjs.Ticker.setFPS(60);
    createjs.Ticker.addEventListener("tick", gameLoop);

    // When game begins, current state will be opening menu (MENU_STATE)
    currentState = constants.MENU_STATE;
    changeState(currentState);
}



// Game Loop
function gameLoop(event): void {
    // Check current state, then update stage
    currentStateFunction();
    stage.update();
}

// This is the state machine function, that allows the game to switch to different 
// screens, or states, depending on where the player is in the game
function changeState(state: number): void {
    // Launch Various "screens"

    switch (state) {
        case constants.MENU_STATE:
            // instantiate menu screen
            currentStateFunction = states.menuState;
            states.menu();
            break;
        case constants.PLAY_STATE:
            // instantiate play screen
            currentStateFunction = states.playState;
            states.play();
            break;
        case constants.GAME_OVER_STATE:
            currentStateFunction = states.gameOverState;
            // instantiate game over screen
            states.gameOver();
            break;
        case constants.WIN_STATE:
            // instantiate win screen
            currentStateFunction = states.gameWinState();
            states.gameWin();
            break;

    }
} 