/// <reference path="../constants.ts" />
/// <reference path="../objects/label.ts" />
/// <reference path="../objects/background.ts" />
/**
File Name: winScreen.ts
Author: Chandan Dadral
Purpose: This file contains all of the elements of the game win screen (high score, play again)
*/
module states {

    // Game loop, update background
    export function gameWinState() {
        background.update();
    }

    // win function, that sets up where everything is on the canvas, and the event listeners
    export function gameWin() {
       
         // Text and labels
        var gameWinMessage: string = "Wow you have finished this game.!!";
        var gameWinLabel: objects.Label;
        var finalScoreMessage: string = "Your final score was: " + scoreboard.score.toString();
        var finalScoreLabel: objects.Label;
        gameWinLabel = new objects.Label(300, 100, gameWinMessage);
        finalScoreLabel = new objects.Label(300, 200, finalScoreMessage);
        // Buttons
        
        var playAgainButton: createjs.Bitmap;
        playAgainButton = new createjs.Bitmap(queue.getResult("playAgainButton"));


        // Set up where the new objects are on the canvas
       
        playAgainButton.x = 180;
        playAgainButton.y = 394;

        // Set up event listeners
        playAgainButton.addEventListener("mouseover", function () {
            playAgainButton.alpha = 0.5;
        });
        playAgainButton.addEventListener("mouseout", function () {
            playAgainButton.alpha = 1;
        });
        playAgainButton.addEventListener("click", function () {
            // If play again button is clicked, destroy all objects and start new game
            background.destroy();
            createjs.Sound.stop();
            game.removeAllChildren;
            game.removeAllEventListeners;
            stage.removeChild(game);
            constants.PLAYER_LIVES = 3;

            currentState = constants.MENU_STATE;
            changeState(currentState);
        });

        // Add all objects to canvas
        game.addChild(gameWinLabel);
        game.addChild(finalScoreLabel);
        game.addChild(playAgainButton);
        // Set mouse cursor to default cursor
        stage.cursor = "default";

        stage.addChild(game);
       
    }

} 