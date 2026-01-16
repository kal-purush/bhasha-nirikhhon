///<reference path="../../reference.d.ts"/>

module DK {
    export class IsoTile extends Phaser.Sprite {

        tileWidth: number;
        tileHeight: number;
        isoPosition: Phaser.Point;

        constructor(game, x, y, key, frame) {
            super(game, x, y, key, frame);

            // TODO: move out
            this.tileWidth = 128;
            this.tileHeight = 64;

            this.isoPosition = new Phaser.Point(x, y);
            this.position = this.isoToScreen();

            // set anchor point based on texture
            var h = (this.texture.height - this.tileHeight) + (this.tileHeight / 2);
            this.anchor.set(0.5, h / this.texture.height);
        }

        isoToScreen() {
            var posX = (this.x - this.y) * this.tileWidth / 2;
            var posY = (this.x + this.y) * this.tileHeight / 2;

            return new Phaser.Point(posX, posY);
        }
    }
}