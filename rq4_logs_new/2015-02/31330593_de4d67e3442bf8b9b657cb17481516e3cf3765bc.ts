/// <reference path="objects/button.ts" />


var canvas;
var stage: createjs.Stage;

// Game Objects 
var game: createjs.Container;
var background: createjs.Bitmap;
var spinButton: objects.Button;
var betMaxButton: objects.Button;
var betOneButton: objects.Button;
var resetButton: objects.Button;
var tiles: createjs.Bitmap[] = [];
var tileContainers: createjs.Container[] = [];

// Game Variables
var playerMoney = 1000;
var winnings = 0;
var jackpot = 5000;
var turn = 0;
var playerBet = 0;
var winNumber = 0;
var lossNumber = 0;
var spinResult;
var fruits = "";
var winRatio = 0;


/* Tally Variables */
var grapes = 0;
var watermalon = 0;
var oranges = 0;
var cherries = 0;
var crown = 0;
var seven = 0;
var seven3 = 0;
var dollor = 0;

function init() {
    canvas = document.getElementById("canvas");
    stage = new createjs.Stage(canvas);
    stage.enableMouseOver(20); // Enable mouse events
    createjs.Ticker.setFPS(60); // 60 frames per second
    createjs.Ticker.addEventListener("tick", gameLoop);

    main();
}

function gameLoop() {


    stage.update(); // Refreshes our stage
}


/* Utility function to reset all fruit tallies */
function resetFruitTally() {
    grapes = 0;
    watermalon = 0;
    oranges = 0;
    cherries = 0;
    crown = 0;
    seven = 0;
    seven3 = 0;
    dollor = 0;
}

// Event handlers

/*function spinButtonOut() {

    spinButton.alpha = 1.0;


}

function spinButtonOver() {
    spinButton.alpha = 0.5;

}*/


function spinReels() {
    // Add Spin Reels code here
    spinResult = Reels();
    fruits = spinResult[0] + " - " + spinResult[1] + " - " + spinResult[2] + " - " + spinResult[3] + " - " + spinResult[4];
    console.log(fruits);


    for (var tile = 0; tile < 5; tile++) {
        if (turn > 0) {
            game.removeChild(tiles[tile]);
        }
        tiles[tile] = new createjs.Bitmap("assets/images/" + spinResult[tile] + ".png");
        tiles[tile].x = 59 + (105 * tile);
        tiles[tile].y = 188;
        
        game.addChild(tiles[tile]);
        console.log(game.getNumChildren());
    }


}

/* Utility function to check if a value falls within a range of bounds */
function checkRange(value, lowerBounds, upperBounds) {
    if (value >= lowerBounds && value <= upperBounds) {
        return value;
    }
    else {
        return !value;
    }
}

/* When this function is called it determines the betLine results.
e.g. Bar - Orange - Banana */
function Reels() {
    var betLine = [" ", " ", " ", " ", " ", " "];
    var outCome = [0, 0, 0, 0, 0, 0];

    for (var spin = 0; spin < 5; spin++) {
        outCome[spin] = Math.floor((Math.random() * 65) + 1);
        switch (outCome[spin]) {
            case checkRange(outCome[spin], 1, 27):  // 41.5% probability
                betLine[spin] = "dollor";
                dollor++;
                break;
            case checkRange(outCome[spin], 28, 37): // 15.4% probability
                betLine[spin] = "grapes";
                grapes++;
                break;
            case checkRange(outCome[spin], 38, 46): // 13.8% probability
                betLine[spin] = "watermalon";
                watermalon++;
                break;
            case checkRange(outCome[spin], 47, 54): // 12.3% probability
                betLine[spin] = "orange";
                oranges++;
                break;
            case checkRange(outCome[spin], 55, 59): //  7.7% probability
                betLine[spin] = "cherry";
                cherries++;
                break;
            case checkRange(outCome[spin], 60, 62): //  4.6% probability
                betLine[spin] = "crown";
                crown++;
                break;
            case checkRange(outCome[spin], 63, 64): //  3.1% probability
                betLine[spin] = "seven";
                seven++;
                break;
            case checkRange(outCome[spin], 65, 65): //  1.5% probability
                betLine[spin] = "3seven";
                seven3++;
                break;
        }
    }
    return betLine;
}


/* This function calculates the player's winnings, if any */
function determineWinnings() {
    if (dollor == 0) {
        if (grapes == 5) {
            winnings = playerBet * 10;
        }
        else if (watermalon == 5) {
            winnings = playerBet * 20;
        }
        else if (oranges == 5) {
            winnings = playerBet * 30;
        }
        else if (cherries == 5) {
            winnings = playerBet * 40;
        }
        else if (crown == 5) {
            winnings = playerBet * 50;
        }
        else if (seven == 5) {
            winnings = playerBet * 75;
        }
        else if (seven3 == 5) {
            winnings = playerBet * 100;
        }
        else if (grapes == 4) {
            winnings = playerBet * 2;
        }
        else if (watermalon == 4) {
            winnings = playerBet * 2;
        }
        else if (oranges == 4) {
            winnings = playerBet * 3;
        }
        else if (cherries == 4) {
            winnings = playerBet * 4;
        }
        else if (crown == 4) {
            winnings = playerBet * 5;
        }
        else if (seven == 4) {
            winnings = playerBet * 10;
        }
        else if (seven3 == 4) {
            winnings = playerBet * 20;
        }
        else {
            winnings = playerBet * 1;
        }

        if (seven3 == 3) {
            winnings = playerBet * 5;
        }
        winNumber++;
       // showWinMessage();
    }
    else {
        lossNumber++;
      //  showLossMessage();
    }

}

function createUI():void {
    // instantiate my background
    background = new createjs.Bitmap("/assets/images/background.png");
    game.addChild(background);

    // Spin Button
    spinButton = new objects.Button("/assets/images/btnSpin.png", 584, 532);
    spinButton.setScale(129/132, 128/131);
    game.addChild(spinButton.getImage());

    spinButton.getImage().addEventListener("click", spinReels);


    // Bet Max Button
    betMaxButton = new objects.Button("/assets/images/btnBetMax.png", 325, 533);
    betMaxButton.setScale(129/132, 128/131);
    game.addChild(betMaxButton.getImage());

    // Bet one Button
    betOneButton = new objects.Button("/assets/images/btnBetOne.png", 454, 533);
    betOneButton.setScale(129/132, 128/131);
    game.addChild(betOneButton.getImage());

    // Reset Button
    resetButton = new objects.Button("/assets/images/btnReset.png", 44, 594);
    resetButton.setScale(95/132, 95/131);
    game.addChild(resetButton.getImage());

    resetButton.getImage().addEventListener("click", function () {
        console.log("reset clicked");
    });
}



// Our Game Kicks off in here
function main() {
    // instantiate my game container
    game = new createjs.Container();
    game.x = 0;
    game.y = 0;

    // Create Slotmachine User Interface
    createUI();


    stage.addChild(game);
}