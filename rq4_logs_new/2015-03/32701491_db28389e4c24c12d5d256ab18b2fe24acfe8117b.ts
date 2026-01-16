/// <reference path="typings/createjs-lib/createjs-lib.d.ts" />
/// <reference path="typings/easeljs/easeljs.d.ts" />
/// <reference path="typings/tweenjs/tweenjs.d.ts" />
/// <reference path="typings/soundjs/soundjs.d.ts" />
/// <reference path="typings/preloadjs/preloadjs.d.ts" />
/// <reference path="typings/stats/stats.d.ts" />

/// <reference path="constants.ts" />
/// <reference path="objects/gameobject.ts" />
/// <reference path="objects/runningpath.ts" />
/// <reference path="objects/hero.ts" />
/// <reference path="objects/enemycar.ts" />
/// <reference path="objects/fuel.ts" />
/// <reference path="objects/button.ts" />
/// <reference path="objects/label.ts" />



    

// Game Variables +++++++++++++++++++++++++++++++++++++++++++++
var stats: Stats = new Stats();
var canvas;
var stage: createjs.Stage;
var assetLoader: createjs.LoadQueue;
var game: createjs.Container;

// Game Objects
var runningPath: objects.Runningpath;
var hero: objects.Hero;
var enemyCar: objects.EnemyCar[] = [];
var fuel: objects.Fuel;

// asset manifest - array of asset objects
var manifest = [
    { id: "hero", src: "assets/images/hero.jpg" },
    { id: "runningPath", src: "assets/images/runningpath.png" },
    { id: "enemyCar", src: "assets/images/enemycar.gif" },
    { id: "fuel", src: "assets/images/fuel.jpg" },
    { id: "engine", src: "assets/audio/engine.ogg" },
    { id: "yay", src: "assets/audio/yay.ogg" },
    { id: "thunder", src: "assets/audio/thunder.ogg" }
];


function preload() {
    assetLoader = new createjs.LoadQueue(); // instantiated assetLoader
    assetLoader.installPlugin(createjs.Sound);
    assetLoader.on("complete", init, this); // event handler-triggers when loading done
    assetLoader.loadManifest(manifest); // loading my asset manifest
}


function init() {
    canvas = document.getElementById("canvas");
    stage = new createjs.Stage(canvas);
    stage.enableMouseOver(20); // Enable mouse events
    createjs.Ticker.setFPS(60); // 60 frames per second
    createjs.Ticker.addEventListener("tick", gameLoop);
    setupStats();

    main();
}

// UTILITY METHODS +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
function setupStats() {
    stats.setMode(0);
    stats.domElement.style.position = 'absolute';
    stats.domElement.style.left = '650px';
    stats.domElement.style.top = '440px';
    document.body.appendChild(stats.domElement);
}

// Calculate the distance between two points ++++++++++++++++++++++++++++++++++++++
function distance(p1: createjs.Point, p2: createjs.Point): number {

    return Math.floor(Math.sqrt(Math.pow((p2.x - p1.x), 2) + Math.pow((p2.y - p1.y), 2)));
}

//CHECK COLLISION  ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
function checkCollision(collider: objects.GameObject) {
    var p1: createjs.Point = new createjs.Point();
    var p2: createjs.Point = new createjs.Point();
    p1.x = hero.x;
    p1.y = hero.y;
    p2.x = collider.x;
    p2.y = collider.y;
    if (distance(p2, p1) < ((hero.height * 0.5) + (collider.height * 0.5))) {
        if (!collider.isColliding) {
            createjs.Sound.play(collider.soundString);
            collider.isColliding = true;

        }
    } else {
        collider.isColliding = false;
    }
}

//GAME LOOP +++++++++++++++++++++++++++++
function gameLoop() {

    stats.begin(); // Begin metering
    runningPath.update();
    hero.update();

    for (var car = constants.ENEMYCAR_NUM; car > 0; car --) {
        enemyCar[car].update();
        checkCollision(enemyCar[car]);
    }

    fuel.update();
    checkCollision(fuel);

    stage.update(); // Refreshes our stage

    stats.end(); // End metering
}

// Our Game Kicks off in here
function main() {
    // Instantiate Game Container
    game = new createjs.Container();

    // Add space to game
    runningPath = new objects.Runningpath();
    game.addChild(runningPath);


    // Add island to game
    hero = new objects.Hero();
    game.addChild(hero);



    for (var car = constants.ENEMYCAR_NUM; car > 0; car--) {
        enemyCar[car] = new objects.EnemyCar();
        game.addChild(enemyCar[car]);
    }

    fuel = new objects.Fuel();
    game.addChild(fuel)

    stage.addChild(game);

}