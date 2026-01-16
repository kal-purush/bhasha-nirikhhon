"use strict";


module M22Shooter {

    export class MainMenu extends Phaser.State {

        start:Phaser.Button

        create() {
            this.game.stage.backgroundColor = '#003212'
            this.start = this.game.add.button(this.game.width / 2 - 54, this.game.height / 2 - 24, 'button', this.startGame)
            this.start.onInputDown.add(this.pressButton, this)
            this.start.onInputUp.add(this.unPressButton, this)
        }

        pressButton() {
            this.start.y += 2;
        }

        unPressButton() {
            this.start.y -= 2;
        }

        startGame() {

            this.game.state.start('Level1', true, false);
        }

    }

}