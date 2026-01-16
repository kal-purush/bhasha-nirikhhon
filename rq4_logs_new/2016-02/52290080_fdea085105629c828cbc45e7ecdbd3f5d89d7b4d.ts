"use strict";


module Ayasha {
    export class Boot extends Phaser.State {

        preload(){
            this.load.image('loader', 'graphics/loader.png');
        }

        create(){
            //  Unless you specifically need to support multitouch I would recommend setting this to 1
            this.input.maxPointers = 1;

            //  Phaser will automatically pause if the browser tab the game is in loses focus. You can disable that here:
            this.stage.disableVisibilityChange = true;

            if (this.game.device.desktop) {
                //  If you have any desktop specific settings, they can go in here
                this.scale.pageAlignHorizontally = true;
                this.scale.scaleMode = Phaser.ScaleManager.SHOW_ALL;
            }
            else {
                //  In this case we're saying "scale the game, no lower than 480x260 and no higher than 1024x768"
                this.scale.scaleMode = Phaser.ScaleManager.SHOW_ALL;
                this.scale.minWidth = 480;
                this.scale.minHeight = 260;
                this.scale.maxWidth = 1024;
                this.scale.maxHeight = 768;
                this.scale.forceLandscape = true;
                this.scale.pageAlignHorizontally = true;
                this.scale.refresh();
            }

            this.game.physics.startSystem(Phaser.Physics.ARCADE)

            this.game.state.start('Preloader', true, false);
        }
    }
}