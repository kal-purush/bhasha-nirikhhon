/// <reference path="../objects/gameobject.ts" />
/// <reference path="../objects/alien.ts" />
/// <reference path="../objects/island.ts" />
/// <reference path="../objects/ocean.ts" />
/// <reference path="../objects/tank.ts" />
/// <reference path="../objects/bullet.ts" />
/// <reference path="../objects/bolt.ts" />
/// <reference path="../objects/explosion.ts" />
module states {
    export function playState(): void {
        tank.update(); //updates tank's position
        //island.update(); //updates island's position
        //ocean.update(); //updates ocean's position

        //if the tank's bullet is onscreen...
        if (tank.bulletOnScreen) {
            tank.bullet.update(); //refresh its position
        } //if ends

        //check if any aliens have hit the top or bottom of the stage
        for (var alien = numberOfAliens - 1; alien >= 0; alien--) {
            if (aliens[alien].hitside) {
                aliensMove(); //if they have call aliensMove
            } //if ends
        } //for ends

        //update the aliens' positions, check if the aliens have collided with the tank or with a 
        //bullet and check if the tank is in firing range of the aliens
        for (var alien = numberOfAliens - 1; alien >= 0; alien--) {
            aliens[alien].update(); //updates aliens' position

            aliens[alien].checkTarget(); //check if the tank is in firing range

            checkCollision(tank, aliens[alien]); //check if the tank and alien have collided

            //if the bullet is onscreen...
            if (tank.bulletOnScreen) {
                checkCollision(tank.bullet, aliens[alien]); //check if the alien collided with it
            } //if ends
        } //for ends

        //update each bolt's position and check if it has collidied with the tank
        for (var bolt = numberOfBolts - 1; bolt >= 0; bolt--) {
            bolts[bolt].update(); //update the bolt's position

            //only check for collision if the bolt wasn't removed from the game in its update
            if (bolts[bolt] != null) {
                checkCollision(tank, bolts[bolt]); //check if it collided with the tank
            } //if ends
        } //for ends

        //check each explosion to see if its time has passed for staying on screen
        for (var explosion = numberOfExplosions - 1; explosion >= 0; explosion--) {
            explosions[explosion].checkTime(); //check if it should stay on screen
        } //for ends
    } //function playState ends

    /*
     * This function sets up the game, creating and placing the game objects and variables
     */
    export function play(): void {
        //set 45 aliens to appear in the game
        numberOfAliens = 45;

        //initialize the number of explosions to 0
        numberOfExplosions = 0;

        //initialize the bolts of explosions to 0
        numberOfBolts = 0;

        //add ocean to game
        ocean = new objects.Ocean();
        stage.addChild(ocean);
    
        //add island to game
        island = new objects.Island();
        stage.addChild(island);

        //add tank to game
        tank = new objects.Tank();
        stage.addChild(tank);

        //add aliens to game
        //numberOfAliens - 1 is necessary to not leave an empty space in the array
        for (var alien = numberOfAliens - 1; alien >= 0; alien--) {
            //aliens will appear in rows of 9
            aliens[alien] = new objects.Alien(1184 + (32 * (Math.floor(alien / 9))),(32 * (alien % 9)));
            stage.addChild(aliens[alien]); //add the alien to the game
        } //for ends

        //set up the game for keyboard input
        //this section checks which key was pressed
        document.addEventListener("keydown", function (event) {
            tank.actionStart(event.keyCode); //send the tank the key that was pressed
        });

        //this section checks which key was released
        document.addEventListener("keyup", function (event) {
            tank.actionEnd(event.keyCode); ///send the tank the key that was pressed
        });

        //play the song
        createjs.Sound.play("song", { loop: -1 });
    } //function play ends

    /*
     * This function calculates the distance between two points.
     */
    function distance(p1: createjs.Point, p2: createjs.Point): number {
        return Math.floor(Math.sqrt(Math.pow((p2.x - p1.x), 2) + Math.pow((p2.y - p1.y), 2)));
    } //function distance ends

    /*
     * This function checks for collisions between two game objects.
     */
    function checkCollision(collider1: objects.GameObject, collider2: objects.GameObject) {
        //create points for each of the objects positions
        var p1: createjs.Point = new createjs.Point();
        var p2: createjs.Point = new createjs.Point();

        //set p1's x and y to collider1's x and y
        p1.x = collider1.x;
        p1.y = collider1.y;

        //set p2's x and y to collider2's x and y
        p2.x = collider2.x;
        p2.y = collider2.y;

        //if the distance of the 2 points is less than half the width of the two objects added,
        //a collision has occured
        if (distance(p2, p1) < ((collider1.width * 0.5) + (collider2.width * 0.5))) {
            //if neither object was already colliding...
            if (!collider1.isColliding && !collider2.isColliding) {
                //set each object to be colliding
                collider1.isColliding = true;
                collider2.isColliding = true;

                //call the collide method of each object
                collider1.collide();
                collider2.collide();
            } //if ends
        } //if ends

        //else the objects were not colliding
        else {
            //set each object to not be colliding
            collider1.isColliding = false;
            collider2.isColliding = false;
        } //else ends
    } //function play ends

    /*
    * This function tells each alien to start moving forward once one alien's hitSide variable has
    * been triggered.
    */
    function aliensMove() {
        //call each alien's moveForward method
        for (var alien = numberOfAliens - 1; alien >= 0; alien--) {
            aliens[alien].moveForward();
        } //for ends
    } //function aliensMove ends
} //module states ends