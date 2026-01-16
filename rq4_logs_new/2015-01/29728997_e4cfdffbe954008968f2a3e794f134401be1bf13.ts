/// <reference path="node_modules\phaser\typescript\phaser.d.ts"/>

module Octopussy {
    export class Game extends Phaser.Game {

        constructor() {
            super(800, 600, Phaser.AUTO, 'content', { preload: this.preload, create: this.create });
        }

        preload() {
            this.load.image('logo', 'img/phaser2.png');
        }

        create() {
            var logo = this.add.sprite(this.world.centerX, this.world.centerY, 'logo');
            logo.anchor.setTo(0.5, 0.5);
            logo.scale.setTo(0.2, 0.2);
            this.add.tween(logo.scale).to({ x: 1, y: 1 }, 2000, Phaser.Easing.Bounce.Out, true);
        }

    }
}

window.onload = () => {
    var game = new Octopussy.Game();
};