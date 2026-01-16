module Tsukthemall {

    export class Boot extends Phaser.State {

        preload() {
            //Preload your loading Bar before loading your assets in Preloader
            this.game.load.image('preloadBar', 'assets/loadbar.png');
        }

        create() {
            this.game.state.start('Preloader', true, false);
        }

    }

}