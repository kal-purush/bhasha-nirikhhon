/*
* COMP2068-Assignment3: Space Infiltrators
*
* Author: Blaine Parr
* Last Modified By: Blaine Parr
* Date Last Modified: March 20th 2015
*
* Description: This game is a javascript remake of Space Invaders. The player controls a tank
* which can fire bullets at aliens moving towards the player. The player can only have 1 bullet
* on the stage at any time. The aliens move downwards and upwards until they hit the edge of the
* stage; upon hitting the edge they will move left and then begin going the opposite direction of
* the one they were going when they hit the edge. The aliens may fire bolts at the player if
* they are in range. The player's goal is to destroy all of the aliens before they get to the
* player.
*
* Revision History:
* v0.1:
* -Created base game.
*
* v0.2:
* -Changed controls from mouse input to keyboard input.
*
* v0.3:
* -Added and implemented gameObject class.
*
* v0.4:
* -Added methods for collision detection and handling.
*
* v0.5:
* -Added firing mechanic to the tank class.
* -Added a song to the game.
*
* v0.6:
* -Added explosion object to the game, for use on collisions.
*
* v0.7:
* -AI for alien's movement complete.
*
* v0.8:
* -Added more aliens to the game.
* -Adjusted speed increase on remaining aliens when an alien is destroyed.
*
* v0.9:
* -Added firing mechanic for the aliens.
*
* v0.10:
* -Added some internal documentation.
*
* v0.11:
* -Fixed errors caused by objects being destroyed before attempting to call one of their methods.
* -Replaced ocean background with a desert background (animation and renaming of class still
* required).
*
* v0.12:
* -Implemented game state system.
*
* v0.13:
* -Added menu to the game (with a working start button).
* -Renamed eveything that said "ocean" to "background."
*
* v0.14:
* -Added an instructions screen to the game.
*
* v0.15:
* -Added a game over screen.
* -implemented scoring system.
* -Added care package object to the game (for bonus points).
*
* v0.16:
* -Added Score, aliens (remaining) and health display to the main game.
* -Added second instruction screen to explain scoring and a couple tips.
*
* v0.17:
* -Added and implemented button class.
* 
* v0.18:
* -Added again button on the game over screen.
*/
/// <reference path="typings/createjs-lib/createjs-lib.d.ts" />
/// <reference path="typings/easeljs/easeljs.d.ts" />
/// <reference path="typings/tweenjs/tweenjs.d.ts" />
/// <reference path="typings/soundjs/soundjs.d.ts" />
/// <reference path="typings/preloadjs/preloadjs.d.ts" />
/// <reference path="constants.ts" />
/// <reference path="objects/gameobject.ts" />
/// <reference path="objects/alien.ts" />
/// <reference path="objects/carePackage.ts" />
/// <reference path="objects/background.ts" />
/// <reference path="objects/tank.ts" />
/// <reference path="objects/bullet.ts" />
/// <reference path="objects/bolt.ts" />
/// <reference path="objects/explosion.ts" />
/// <reference path="objects/button.ts" />
/// <reference path="states/play.ts" />
/// <reference path="states/menu.ts" />
/// <reference path="states/gameover.ts" />
/// <reference path="states/instructions.ts" />

//Canvas and Asset Objects
var canvas;
var stage: createjs.Stage;
var assetLoader;

//Game Objects and Variables
var tank: objects.Tank;
var carePackage: objects.CarePackage;
var aliens: objects.Alien[] = [];
var background: objects.Background;
var numberOfAliens: number;
var explosions: objects.Explosion[] = [];
var numberOfExplosions: number;
var bolts: objects.Bolt[] = [];
var numberOfBolts: number;
var startButton: objects.Button;
var instructionsButton: objects.Button;
var nextButton: objects.Button;
var againButton: objects.Button;
var menuScreen: createjs.Bitmap;
var instructionsScreen: createjs.Bitmap;
var instructionsScreen2: createjs.Bitmap;
var scoreBoard: createjs.Bitmap;
var score: number;
var scoreText: createjs.Text;
var healthText: createjs.Text;
var aliensText: createjs.Text;
var missionOutcome: string;
var missionOutcomeText: createjs.Text
    ;
//state objects
var currentState: number;
var currentStateFunction: any;

//asset manifest - array of asset objects
var manifest = [
    { id: "alien", src: "assets/images/Alien.png" },
    { id: "carePackage", src: "assets/images/CarePackage.png" },
    { id: "background", src: "assets/images/Background.png" },
    { id: "tank", src: "assets/images/Tank.png" },
    { id: "bullet", src: "assets/images/Bullet.png" },
    { id: "explosion", src: "assets/images/Explosion.png" },
    { id: "bolt", src: "assets/images/Bolt.png" },
    { id: "song", src: "assets/audio/Conquest.ogg" },
    { id: "startButton", src: "assets/images/StartButton.png" },
    { id: "instructionsButton", src: "assets/images/InstructionsButton.png" },
    { id: "nextButton", src: "assets/images/NextButton.png" },
    { id: "againButton", src: "assets/images/AgainButton.png"}
];

/*
 * This function preloads all of the assets in the game, making it ready before the game is
 * launched.
 */
function preload() {
    assetLoader = new createjs.LoadQueue(); // instantiated assetLoader
    assetLoader.installPlugin(createjs.Sound);
    assetLoader.on("complete", init, this); // event handler-triggers when loading done
    assetLoader.loadManifest(manifest); // loading my asset manifest
} //function preload ends

/*
 * This function initializes the game by setting up the canvas, FPS and enabling mouseover
 */
function init() {
    canvas = document.getElementById("canvas");
    stage = new createjs.Stage(canvas);
    stage.enableMouseOver(20); // Enable mouse events
    createjs.Ticker.setFPS(60); // 60 frames per second
    createjs.Ticker.addEventListener("tick", gameLoop);
    currentState = constants.MENU_STATE;
    changeState(currentState);
} //function init ends
/*
 * This function loops and updates the game as it is being played.
 */

function gameLoop() {
    currentStateFunction();
    stage.update(); // Refreshes our stage
} //function gameLoop ends

function changeState(state) {
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
            states.gameOver();
            break;
        case constants.INSTRUCTIONS_STATE:
            currentStateFunction = states.instructionsState;
            states.instructions();
            break;
    } //switch ends
} //function changeState ends