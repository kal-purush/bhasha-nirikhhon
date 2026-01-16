/// <reference path="../constants.ts" />
/**
File Name: bullet.ts
Author: Chandan Dadral
Purpose: This file contains all details to initalize a bullet object
*/
module objects {
    export class Bullet {
        image: createjs.Bitmap;
        stage: createjs.Stage;
        game: createjs.Container;
        width: number;
        height: number;

        constructor(stage: createjs.Stage, game: createjs.Container) {
            this.stage = stage;
            this.game = game;
            this.image = new createjs.Bitmap(queue.getResult("bullet"));
            this.width = this.image.getBounds().width;
            this.height = this.image.getBounds().height;

        }

        // Function to fire bullet. Sets where bullet will begin, and adds it to stage
        fireBullet() {
            this.image.x = stage.mouseX + 5;
            this.image.y = stage.mouseY + 5;
            game.addChild(this.image);
        }

        // Function to update position of bullet.
        bulletUpdate() {
            this.image.x += constants.BULLET_SPEED;
            if (this.image.x > 640) {
                this.bulletReset();
            }
        }

        // Function to reset bullet off screen, and destroy it
        bulletReset() {
            this.image.y = 700;
            game.removeChild(this.image);
        }
    }
}  