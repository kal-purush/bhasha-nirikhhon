/// <reference path="references.ts" />

module Numbercatch {
    export class Being extends Phaser.Sprite{
        coordinateX;
        coordinateY;
        static BEING_TYPE_GHOST = 'ghost';
        static BEING_TYPE_CHARACTER = 'character';
        beingType;
        gameScene;

        public constructor(game, x, y, gameScene) {
            super(game, x, y);
            this.gameScene = gameScene;

        }


        setCoordinates(x, y) {
            this.coordinateX = x;
            this.coordinateY = y;
        }
    };
}
