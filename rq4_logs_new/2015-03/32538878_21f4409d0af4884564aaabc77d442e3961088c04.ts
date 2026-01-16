/// <reference path="../constants.ts" />
/// <reference path="../objects/label.ts" />
/// <reference path="../objects/background.ts" />

/**
File Name: mainMenuScreen.ts
Author: Chandan Dadral
Purpose: This file contains all of the elements of the main menu screen (start game, instructions) and the game logo
*/
module states {
    // Game Loop function, updates background
    export function menuState() {
        background.update();
    }

    // main menu function, that sets up where everything is on the canvas, and the event listeners
    export function menu() {
        // Buttons
        var gameLogo: createjs.Bitmap;
        var playButton: createjs.Bitmap;
        var instructionsButton: createjs.Bitmap;
        var okButton: createjs.Bitmap;

        var soundtrack = createjs.Sound.play("mainMenu", { loop: 2 });
        
        // Text and labels
        var instructionsMessage: string = "In this game, barry's labortary was attacked by enemies, "
            + "you need to avoid from the missles and save your life "
            + "everytime you win 1000 points, then you get one more live, you need to survive. !"
            + "Lets See how many points you can Get!";
        var instructionsText: createjs.Text;

        // initialize new objects
        game = new createjs.Container();
        gameLogo = new createjs.Bitmap(queue.getResult("gameLogo"));
        background = new objects.Background(stage, game);
        playButton = new createjs.Bitmap(queue.getResult("playButton"));
        instructionsButton = new createjs.Bitmap(queue.getResult("instructionsButton"));
        okButton = new createjs.Bitmap(queue.getResult("okButton"));
        instructionsText = new createjs.Text(instructionsMessage, constants.LABEL_FONT, constants.LABEL_COLOUR);

       
       

        // Set up where the new objects are on the canvas
       
        gameLogo.x = 110;
        gameLogo.y = 54;
        okButton.x = 284;
        okButton.y = 350;
        okButton.visible = false;
        playButton.x = 100;
        playButton.y = 385;
        instructionsButton.x = 350;
        instructionsButton.y = 385;
        instructionsText.y = 50;
        instructionsText.x = 25;
        instructionsText.lineHeight = 40;
        instructionsText.lineWidth = 630;
        instructionsText.visible = false;
       
       
        
        // set up event listeners for all buttons
        playButton.addEventListener("mouseover", function () {
            playButton.alpha = 0.5;
            createjs.Sound.play("hover");
        });
        playButton.addEventListener("mouseout", function () {
            playButton.alpha = 1;
        });
        playButton.addEventListener("click", function () {
            // If play button is clicked, destory all objects and start game
            createjs.Sound.play('gameStartAudio');
            background.destroy();
            game.removeAllChildren;
            game.removeAllEventListeners;
            stage.removeChild(game);
            currentState = constants.PLAY_STATE;
            changeState(currentState);
            soundtrack.stop();

        });
        instructionsButton.addEventListener("mouseover", function () {
            instructionsButton.alpha = 0.5;
            createjs.Sound.play("hover");
        });
        instructionsButton.addEventListener("mouseout", function () {
            instructionsButton.alpha = 1;
        });
        //when instruction button is clicked then it shows the instructions on the screen
        instructionsButton.addEventListener("click", function () {

            gameLogo.visible = false;
            playButton.visible = false;
            instructionsButton.visible = false;
            instructionsText.visible = true;

            okButton.visible = true;
        });
        okButton.addEventListener("mouseover", function () {
            okButton.alpha = 0.5;
            createjs.Sound.play("hover");
        });
        okButton.addEventListener("mouseout", function () {
            okButton.alpha = 1;
        });

        //ok button takes back to the Main Menu Screen
        okButton.addEventListener("click", function () {
            gameLogo.visible = true;

            playButton.visible = true;
            instructionsButton.visible = true;
            instructionsText.visible = false;

            okButton.visible = false;
        });
        // Add all objects to canvas
       
        game.addChild(gameLogo);
        game.addChild(playButton);
        game.addChild(instructionsButton);
        game.addChild(instructionsText);
        game.addChild(okButton);
        
      
        // Set mouse cursor to default cursor
        stage.cursor = "default";

        stage.addChild(game);
    }



}   