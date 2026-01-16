/// <reference path="definitions/phaser.d.ts"/>
/// <reference path="states/Preload.ts" />
/// <reference path="states/MainMenu.ts" />
/// <reference path="states/Gameover.ts" />
/// <reference path="states/Play.ts" />

class Game extends Phaser.Game {
    constructor() {
        super(800, 600, Phaser.AUTO, 'phaser-template', null);

        this.state.add('boot', Boot);
        this.state.add('gameover', Gameover);
        this.state.add('menu', MainMenu);
        this.state.add('play', Play);
        this.state.add('preload', Preload);

        this.state.start('boot');
    }
}