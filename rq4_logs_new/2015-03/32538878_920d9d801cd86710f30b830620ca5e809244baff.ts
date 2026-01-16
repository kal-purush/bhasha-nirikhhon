
/// <reference path="../constants.ts" />
/// <reference path="../objects/barry.ts" />
/// <reference path="../objects/missles.ts" />
/// <reference path="../objects/coins.ts" />
/// <reference path="../objects/background.ts" />
/// <reference path="../objects/scoreboard.ts" />
/// <reference path="../managers/collision.ts" />


/**
File Name: gamePlay.ts
Author: Chandan Dadral
Purpose: This file contains all of the elements of the game play screen it changes the score and destorys the objects
*/
module states {

    export function playState() {
        background.update();
        coins.update();

        //update the position of the missles
        for (var count = 0; count < constants.ENEMY_NUM; count++) {
            missles[count].update();
        }

       
        //check the collisions
        managers.collisionCheck();

        barry.update();
        scoreboard.update();

        // If lives is 0 or lower, destroy all objects and go to gameover screen
        if (scoreboard.lives <= 0) {
            states.getHighScore(scoreboard.score);
            stage.removeChild(game);
            barry.destroy();
            background.destroy();
            coins.destroy();

            game.removeAllChildren();
            game.removeAllEventListeners();
            stage.removeChild(game);

            //changes the game state
            currentState = constants.GAME_OVER_STATE;
            changeState(currentState);
        }
        //if player score reaches 7000 then player wins the game
        if (scoreboard.score == 7000) {

            barry.destroy();
            coins.destroy();
            for (var count = 0; count < constants.ENEMY_NUM; count++) {
                missles[count].destroy();
            }

            currentState = constants.WIN_STATE;
            changeState(currentState);
        }
    }

    // play state Function
    export function play(): void {
        game = new createjs.Container();
        // Set mouse cursor to none (Barry will take place of cursor)
        stage.cursor = "none";
        background = new objects.Background(stage, game);
        coins = new objects.Coins(stage, game);
        barry = new objects.Barry(stage, game);



        for (var count = 0; count < constants.ENEMY_NUM; count++) {
            missles[count] = new objects.Missle(stage, game);
        }

        scoreboard = new objects.scoreBoard(stage, game);

        stage.addChild(game);

    }
}  