
/// <reference path="../constants.ts" />
/// <reference path="../objects/label.ts" />
/// <reference path="../objects/background.ts" />

/**
File Name: gameOverScreen.ts
Author: Chandan Dadral
Purpose: This file contains all of the elements of the game over screen (high score, play again)
*/
module states {
    // variable to keep high score in
    var highScore: number = 0;

    // Game loop, update lava in background
    export function gameOverState() {
        background.update();
    }

    // game over function, that sets up where everything is on the canvas, and the event listeners
    export function gameOver() {

        // Text and labels
        var gameOver: createjs.Bitmap;
        var gameOverMessage: string = "You are out of lives! Game Over!";

        var finalScoreMessage: string = "Your score was: " + highScore.toString();
        var finalScoreLabel: objects.Label;

        finalScoreLabel = new objects.Label(330, 200, finalScoreMessage);
        
        // Play Again Buttons
        var playAgainButton: createjs.Bitmap;
        playAgainButton = new createjs.Bitmap(queue.getResult("playAgainButton"));

        // game and background variables
        game = new createjs.Container();
        background = new objects.Background(stage, game);
        gameOver = new createjs.Bitmap(queue.getResult("gameOver"));

        // Set up where the new objects are on the canvas
        gameOver.x = 107;
        gameOver.y = 57;
        playAgainButton.x = 180;
        playAgainButton.y = 394;
        finalScoreLabel.x = 280;
        finalScoreLabel.y = 275;

        // Set up event listeners
        playAgainButton.addEventListener("mouseover", function () {
            playAgainButton.alpha = 0.5;
            createjs.Sound.play("hover");
        });

        playAgainButton.addEventListener("mouseout", function () {
            playAgainButton.alpha = 1;
        });

        playAgainButton.addEventListener("click", function () {
            // If play again button is clicked, destroy all objects and start new game from the Play Screen
            background.destroy();
            game.removeAllChildren;
            game.removeAllEventListeners;
            stage.removeChild(game);
            currentState = constants.MENU_STATE;
            changeState(currentState);
            createjs.Sound.stop();

        });

        // Add all objects to canvas
       
        game.addChild(gameOver);
        game.addChild(finalScoreLabel);
        game.addChild(playAgainButton);


        // Set mouse cursor to default cursor
        stage.cursor = "default";

        stage.addChild(game);

    }

    // function to retrieve high score
    export function getHighScore(score: number) {
        highScore = score;
    }

}