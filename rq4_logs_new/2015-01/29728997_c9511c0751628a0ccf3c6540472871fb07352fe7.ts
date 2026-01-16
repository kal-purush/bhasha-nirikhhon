/// <reference path="framework/fullscreenState.ts"/>

module Octopussy {
    export class Story extends FullscreenState {

        private storyText: string[] = 
        [
                'Hier könnte ihre Werbung stehen!',
                'Echt jetzt!',
                'Dann eben nicht!',
                'Fuuuuuu!!!'
        ];
        private storyState: number = 0;

        private textElement: Phaser.Text;

        create() {
            super.create();

            this.stage.setBackgroundColor('#44FF10');

            var style = { font: "65px Arial", fill: "#ff0044", align: "center" };
            this.textElement = this.add.text(this.game.world.centerX, this.game.world.centerY, "", style);
            this.input.onDown.add(this.next, this);
        }

        private changeText(text: string) {

            this.textElement.text = text;
            this.textElement.anchor.x = Math.round(this.textElement.width * 0.5) / this.textElement.width;
            this.textElement.anchor.y = Math.round(this.textElement.height * 0.5) / this.textElement.height;
        } 

        private next() {

            if (this.storyState == this.storyText.length) {

                this.game.state.start('Level');
            }

            this.changeText(this.storyText[this.storyState]);
            this.storyState++;
        }
    }
} 