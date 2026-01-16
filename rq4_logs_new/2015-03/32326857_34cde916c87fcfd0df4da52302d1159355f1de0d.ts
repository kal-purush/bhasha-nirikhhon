/// <reference path="../objects/scoreboard.ts" />
/// <reference path="../objects/ring.ts" />
/// <reference path="../objects/nemo.ts" />
/// <reference path="../objects/label.ts" />
/// <reference path="../objects/gameobject.ts" />
/// <reference path="../objects/button.ts" />
/// <reference path="../objects/bee.ts" />
/// <reference path="../objects/background.ts" />
/// <reference path="../../constants.ts" />





module states {
    // GAME OVER STATE CLASS
    export class GameOver {
        // Game Objects 
        public game: createjs.Container;
        public background: objects.Background;
        public gameOverLabel: objects.Label;
        public gameWinLabel: objects.Label;
        public finalScoreLabel: objects.Label;
        public highScoreLabel: objects.Label;
        public tryAgainButton: objects.Button;
        public tryAgain: boolean = false;

        // CONSTRUCTOR ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        constructor() {
            // Instantiate Game Container
            this.game = new createjs.Container();

            //Ocean object
            this.background = new objects.Background();
            this.game.addChild(this.background);

            //Game Over Label
            this.gameOverLabel = new objects.Label(320, 40, finalText);
            this.gameOverLabel.font = "60px Consolas";
            this.gameOverLabel.regX = this.gameOverLabel.getMeasuredWidth() * 0.5;
            this.gameOverLabel.regY = this.gameOverLabel.getMeasuredLineHeight() * 0.5;
            this.game.addChild(this.gameOverLabel);

            this.gameWinLabel = new objects.Label(320, 40, finalText.toString());
            this.gameWinLabel.font = "60px Consolas";
            this.gameWinLabel.regX = this.gameWinLabel.getMeasuredWidth() * 0.5;
            this.gameWinLabel.regY = this.gameWinLabel.getMeasuredLineHeight() * 0.5;
            this.game.addChild(this.gameWinLabel);

            //Final Score Label
            this.finalScoreLabel = new objects.Label(320, 120,("FINAL SCORE: " + finalScore));
            this.game.addChild(this.finalScoreLabel);

            //Final Score Label
            this.highScoreLabel = new objects.Label(320, 220,("HIGH SCORE: " + highScore));
            this.game.addChild(this.highScoreLabel);

            //Try Again Button
            this.tryAgainButton = new objects.Button("tryAgainButton", 320, 350);
            this.game.addChild(this.tryAgainButton);
            this.tryAgainButton.on("click", this.tryAgainClicked, this);

           

            // Add Game Container to Stage
            stage.addChild(this.game);
        } // Constructor

        public tryAgainClicked() {
            this.tryAgain = true;
        }

        // PUBLIC METHODS +++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        public update() {

            this.background.update();

            if (this.tryAgain) {
                this.game.removeAllChildren();
                stage.removeChild(this.game);
                currentState = constants.PLAY_STATE;
                stateChanged = true;
            }

            stage.update(); // Refreshes our stage

        } // Update Method

    } // Game Over Class


} // States Module 