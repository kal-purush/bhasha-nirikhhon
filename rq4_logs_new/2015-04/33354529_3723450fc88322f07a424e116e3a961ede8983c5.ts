/// <reference path="references.ts" />

module Numbercatch {
    export class Character extends Phaser.Sprite{
        coordinateX;
        coordinateY;

        public constructor(game, x, y) {
            super(game, x, y);

            Phaser.Sprite.call(this, game, 0, 0, 'character1');
            this.anchor.setTo(.5,.5);
        }


        setPosition(x, y) {
            this.coordinateX = x;
            this.coordinateY = y;
            this.x = x*this.width*1.5 + ((y%2 == 0) ? (this.width/2) : (this.width/2)*2.5);
            this.y = (y +1)*(this.height/2);
        }
    };
}
