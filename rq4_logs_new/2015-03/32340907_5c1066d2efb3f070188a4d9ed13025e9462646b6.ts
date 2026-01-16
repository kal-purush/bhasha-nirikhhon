/// <reference path="typings/createjs-lib/createjs-lib.d.ts" />
/// <reference path="typings/easeljs/easeljs.d.ts" />
/// <reference path="typings/tweenjs/tweenjs.d.ts" />
/// <reference path="typings/soundjs/soundjs.d.ts" />
/// <reference path="typings/stats/stats.d.ts" />
/// <reference path="constants.ts" />

/// <reference path="objects/plane.ts" />
/// <reference path="objects/island.ts" />
/// <reference path="objects/cloud.ts" />
/// <reference path="objects/ocean.ts" />
/// <reference path="objects/bullet.ts" />

var stats: Stats = new Stats();
var canvas;
var stage: createjs.Stage;
var assetLoader: createjs.LoadQueue;


//Game objects
var plane: objects.Plane;
var collectible: objects.Island;
var enemies: objects.Cloud[] = [];
var sky: objects.Ocean;
var bullets: objects.Bullet[] = [];
var bullet: objects.Bullet;

// asset manifest - array of asset objects
var manifest = [
    { id: "enemy", src: "assets/images/enemy.png" },
    { id: "collectible", src: "assets/images/collectible.png" },
    { id: "background", src: "assets/images/sky.png" },
    { id: "plane", src: "assets/images/plane.png" },
    { id: "bullet", src: "assets/images/bullet.png" },
    { id: "engine", src: "assets/audio/engine.ogg" },
    { id: "collect", src: "assets/audio/yay.ogg" },
    { id: "damage", src: "assets/audio/thunder.ogg" }
];

function preload1() {
    assetLoader = new createjs.LoadQueue(); // instantiated assetLoader
    assetLoader.installPlugin(createjs.Sound);
    assetLoader.on("complete", init, this); // event handler-triggers when loading done
    assetLoader.loadManifest(manifest); // loading my asset manifest
} //function preload ends

//Initialize the game
function init1() {
    setupStats();
    canvas = document.getElementById("canvas");
    stage = new createjs.Stage(canvas);
    stage.enableMouseOver(20); // Enable mouse events
    createjs.Ticker.setFPS(60); // 60 frames per second
    createjs.Ticker.addEventListener("tick", gameLoop);

    main();
} //function init ends

//Tick event Function
function gameLoop1() {
    stats.begin();
    bulletUpdate();
    
    plane.update(); //updates plane's position
    collectible.update(); //updates island's position
    sky.update(); //updates ocean's position
    checkCollision(plane,collectible);
    for (var enmey = 3; enmey > 0; enmey--) {
        //checkCollision(plane, enemies[enmey]);
        plane.isColliding = false; 
        enemies[enmey].update(); //updates cloud's position
    } //for ends
    // if (bullets.length >= 1) {
    var bulletAmount = bullets.length - 1;
    for (var i = bulletAmount; i >= 0; i--) {
        bullets[i].update();
    }
    stage.update(); // Refreshes our stage
    stats.end();
} //function gameLoop ends

//Event Handlers ###############################################################

function stageButtonClick() {
    bullet = new objects.Bullet();
    bullet.x = 55;
    bullet.y = stage.mouseY;

    bullets.unshift(bullet);
    stage.addChild(bullets[0]);
}

//UTILITY METHODS ###################################################

//Create an fps display
function setupStats() {
    stats.setMode(1);
    stats.domElement.style.position = 'absolute';
    stats.domElement.style.left = '810px';
    stats.domElement.style.top = '0';
    document.body.appendChild(stats.domElement);
}

//Calculate distance between two points
function distance(p1: createjs.Point, p2: createjs.Point): number {

    return Math.floor(Math.sqrt(Math.pow((p2.x - p1.x), 2) + Math.pow((p2.y - p1.y), 2)));
}//END distance

//Collision detection function between the player and game objects
function checkCollision(collider1: objects.GameObject, collider2: objects.GameObject) {
    var p1: createjs.Point = new createjs.Point;
    var p2: createjs.Point = new createjs.Point;
    p1.x = collider1.x;
    p1.y = collider1.y;
    p2.x = collider2.x;
    p2.y = collider2.y;
    if (distance(p1, p2) < ((collider1.width * 0.5) + (collider2.width * 0.5))) {
        if (!collider2.isColliding) {
            createjs.Sound.play(collider2.soundString);
            collider1.isColliding = true;
            collider2.isColliding = true;
        }
    }
    else {
        collider1.isColliding = false;
        collider2.isColliding = false;
    }
}//END checkCollision

//method to check if bullets have hit enemies and update movement of bullets
function bulletUpdate() {
   // if (bullets.length >= 1) {
        var bulletAmount = bullets.length - 1;
        for (var i = bulletAmount; i >= 0; i--) {

            for (var x = 3; x > 0; x--) {
                checkCollision(bullets[i], enemies[x]);
                if (bullets[i].isColliding) {
                    enemies[x].update;    
                    stage.removeChild(bullets[i]);
                    bullets = bullets.splice(i, 1);
                    x = 0;
                }
            }       
    
        }
    }


//}//END bulletUpdate

//Initialization ##################################################################

// Our Game Kicks off in here
function main() {
    //add ocean to game
    sky = new objects.Ocean();
    stage.addChild(sky);

    //add island to game
    collectible = new objects.Island();
    stage.addChild(collectible);

    //add plane to game
    plane = new objects.Plane();
    stage.addChild(plane);

    //add clouds to game
    for (var cloud = 3; cloud > 0; cloud--) {
        enemies[cloud] = new objects.Cloud(); //updates cloud's position
        stage.addChild(enemies[cloud]);
    } //for ends

    //activates the mouse click by firing
    stage.addEventListener("click", stageButtonClick);
} //function main ends