/// <reference path="typings/createjs-lib/createjs-lib.d.ts" />
/// <reference path="typings/easeljs/easeljs.d.ts" />
/// <reference path="typings/tweenjs/tweenjs.d.ts" />
/// <reference path="typings/soundjs/soundjs.d.ts" />
/// <reference path="typings/preloadjs/preloadjs.d.ts" />
/// <reference path="typings/stats/stats.d.ts" />


/// <reference path="objects/background.ts" />
/// <reference path="objects/nemo.ts" />
/// <reference path="objects/bee.ts" />
/// <reference path="objects/ring.ts" />
/// <reference path="objects/gameobject.ts" />


var stats: Stats = new Stats();
var canvas;
var stage: createjs.Stage;
var assetLoader: createjs.LoadQueue;

// Game Objects
var nemo: objects.Nemo;
var ring: objects.Ring;
var bee: objects.Bee[] = [];
var background: objects.Background;



// asset manifest - array of asset objects
var manifest = [
    { id: "bee", src: "assets/images/bee.png" },
    { id: "ring", src: "assets/images/ring.png" },
    { id: "background", src: "assets/images/background.png" },
    { id: "nemo", src: "assets/images/nemo.png" },
    { id: "engine", src: "assets/audio/engine.ogg" },
    { id: "yay", src: "assets/audio/yay.ogg" },
    { id: "thunder", src: "assets/audio/thunder.ogg" }

];

// Game Objects 

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

// Calculate the distance between two points
function distance(p1: createjs.Point, p2: createjs.Point):number {

    return Math.floor(Math.sqrt(Math.pow((p2.x - p1.x),2) + Math.pow((p2.y -p1.y),2)));
}

function checkCollision(collider: objects.GameObject) {
    var p1: createjs.Point = new createjs.Point();
    var p2: createjs.Point = new createjs.Point();
    p1.x = nemo.x;
    p1.y = nemo.y;
    p2.x = collider.x;
    p2.y = collider.y;
    if (distance(p2, p1) < ((nemo.height * 0.5) + (collider.height * 0.5))) {
        if (!collider.isColliding) {
            createjs.Sound.play(collider.soundString);
            collider.isColliding = true;
        }
    } else {
        collider.isColliding = false;
    }
}


function gameLoop() {
    stats.begin(); // Begin metering

    background.update();
    nemo.update();
    ring.update();

    for (var cloud = 3; cloud > 0; cloud--) {
        bee[cloud].update();
        checkCollision(bee[cloud]);
    }

    checkCollision(ring);


    stage.update(); // Refreshes our stage

    stats.end(); // End metering
}





// Our Game Kicks off in here
function main() {
    // Add ocean to game
    background = new objects.Background();
    stage.addChild(background);


    // Add island to game
    ring = new objects.Ring();
    stage.addChild(ring);


    // Add plane to game
    nemo = new objects.Nemo();
    stage.addChild(nemo);

    // Add clouds to game
    for (var cloud = 3; cloud > 0; cloud--) {
        bee[cloud] = new objects.Bee();
        stage.addChild(bee[cloud]);
    }



    
}