/// <reference path="framework/fullscreenState.ts"/>

module Octopussy {
    export class Intro extends FullscreenState {

        preload() {

            this.load.image('logo_cupworks', 'assets/logo_cupworks.png');
        }

        create() {
            super.create();

            var logo = this.add.sprite(this.world.centerX, this.world.centerY, 'logo_cupworks');
            logo.anchor.setTo(0.5, 0.5);
            logo.alpha = 0;

            var tween = this.add.tween(logo).to({ alpha: 1 }, 2000, Phaser.Easing.Bounce.InOut, true);
            tween.onComplete.add(this.changeState, this);
        }

        private changeState() {

            this.game.state.start('MainMenu');
        }
    }
}  