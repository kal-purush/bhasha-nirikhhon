///<reference path="../reference.d.ts"/>

module DK {
    export class Game extends Phaser.Game {

        constructor() {
            var dpr = 1, //window.devicePixelRatio,
                width = 768 * dpr,
                height = 1024 * dpr;

            super(width, height, Phaser.CANVAS, 'content', null);

            /*
             this.canvas.width *= window.devicePixelRatio;
             this.canvas.height *= window.devicePixelRatio;

             this.canvas.style.width = width + 'px';
             this.canvas.style.height = height + 'px';
             */

            this.state.add('Preloader', PreloaderState, false);
            this.state.add('Game', GameState, false);

            this.state.start('Preloader');
        }
    }
}

var game;