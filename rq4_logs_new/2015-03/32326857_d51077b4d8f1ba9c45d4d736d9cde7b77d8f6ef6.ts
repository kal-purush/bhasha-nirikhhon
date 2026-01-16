/// <reference path="../objects/button.ts" />
/// <reference path="../objects/background.ts" />
/// <reference path="../objects/bee.ts" />
/// <reference path="../objects/gameobject.ts" />
/// <reference path="../objects/label.ts" />
/// <reference path="../objects/nemo.ts" />
/// <reference path="../objects/ring.ts" />
/// <reference path="../objects/scoreboard.ts" />
/// <reference path="../game.ts" />
/// <reference path="../../constants.ts" />


module states {
    export class Instruction {

        public game: createjs.Container;
        public background: objects.Background;
        public instructionText: createjs.Text;
        public backButton: objects.Button;
        public play: boolean = false;

        constructor() {
            
            // Instantiate Game Container
            this.game = new createjs.Container();

            //Ocean object
            this.background = new objects.Background();
            this.game.addChild(this.background);

            var instructionsMessage: string = "Welcome to Nemo Fighter game,Nemo's was attacked by enemies, "
                + "you need to save nemo from the bees and save his life. "
                + "In order to win this game,you need to collect 5000 points!"
                + "Steer with the mouse, Lets See how many points you can Get!";

            this.instructionText = new createjs.Text(instructionsMessage, constants.LABEL_FONT, constants.LABEL_COLOUR);

            this.instructionText.y = 15;
            this.instructionText.x = 25;
            this.instructionText.lineHeight = 40;
            this.instructionText.lineWidth = 630;
            this.game.addChild(this.instructionText);
          
           
            //back Button
            this.backButton = new objects.Button("backButton", 300, 420);
            this.backButton.on("click", this.backClicked, this);

            this.game.addChild(this.backButton);

            

            stage.addChild(this.game);


        }

        public backClicked() {
            this.play = true;
        }

        public update() {

            this.background.update();
            
            // instructionText.visible = true;  
            if (this.play) {
                this.game.removeAllChildren();
                stage.removeChild(this.game);
                currentState = constants.MENU_STATE;
                stateChanged = true;
            }
            stage.update();

        }
    }
} 