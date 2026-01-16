/// <reference path="objects/button.ts" />


var canvas;
var stage: createjs.Stage;

// GAME CONSTANTS
var NUM_REELS: number = 3;

// Game Objects 
var game: createjs.Container;
var background: createjs.Bitmap;
var jackpotImg: createjs.Bitmap;
var spinButton: objects.Button;
var powerButton: objects.Button;
var resetButton: objects.Button;
var betTenButton: objects.Button;
var betMaxButton: objects.Button;
var tiles: createjs.Bitmap[] = [];
var tileContainers: createjs.Container[] = [];

// Game Variables
var playerMoney = 1000;
var winnings = 0;
var jackpot = 5000;
var turn = 0;
var playerBet = 20;
var winNumber = 0;
var lossNumber = 0;
var spinResult;
var fruits = "";
var winRatio = 0;
var winningText = new createjs.Text("0", "23px play", "#000000");
var pointsWonText = new createjs.Text("0", "23px play", "#000000");
var scoreText = new createjs.Text("000000", "23px play", "#000000");
var jackpotText = new createjs.Text("Good Luck", "48px jiggler", "#000000");
var onOffText = new createjs.Text("", "37px play", "#000000");


/* Tally Variables */
var grapes = 0;
var bananas = 0;
var oranges = 0;
var cherries = 0;
var bars = 0;
var bells = 0;
var sevens = 0;
var blanks = 0;

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
    bananas = 0;
    oranges = 0;
    cherries = 0;
    bars = 0;
    bells = 0;
    sevens = 0;
    blanks = 0;
}


function spinReels() {
    // Add Spin Reels code here
    resetFruitTally();
    spinResult = Reels();
    fruits = spinResult[0] + " - " + spinResult[1] + " - " + spinResult[2];
    determineWinnings();
    playerMoney = playerMoney + winnings;
    console.log(playerMoney + "Number of grapes" + grapes);
    console.log("Winning Money" + winnings);
    game.removeChild(scoreText);
    game.removeChild(pointsWonText);
    game.removeChild(winningText);
    game.removeChild(jackpotText);
    game.removeChild(jackpotImg);

    scoreText = new createjs.Text(playerMoney.toString(), "23px play", "#000000");
    scoreText.x = 92
    scoreText.y = 411;
    game.addChild(scoreText);

    winningText = new createjs.Text(winNumber.toString(), "23px play", "#000000");
    winningText.x = 330
    winningText.y = 411;
    game.addChild(winningText);

    pointsWonText = new createjs.Text(winnings.toString(), "23px play", "#000000");
    pointsWonText.x = 213
    pointsWonText.y = 411;
    game.addChild(pointsWonText);


    if (grapes == 2) {
        console.log("entered here");
        jackpotImg = new createjs.Bitmap("assets/images/jackpot.gif");
        jackpotImg.x = 108;
        jackpotImg.y = 111;
        game.addChild(jackpotImg);

    }
    else if (winnings == 0) {
        jackpotText = new createjs.Text("Snap! Pigs", "48px jiggler", "#000000");
        //postion winning text
        jackpotText.x = 108
        jackpotText.y = 120;
        game.addChild(jackpotText);
    } else
    {
        jackpotText = new createjs.Text("Woo! Birds You won", "38px jiggler", "#000000");
        //postion winning text
        jackpotText.x = 71
        jackpotText.y = 120;
        game.addChild(jackpotText);
    }

    

    // Iterate over the number of reels
    for (var index = 0; index < NUM_REELS; index++) {
        tileContainers[index].removeAllChildren();
        tiles[index] = new createjs.Bitmap("assets/images/" + spinResult[index] + ".png");
        tileContainers[index].addChild(tiles[index]);
    }
    winnings = 0;
    playerMoney = playerMoney - (playerBet + 30);
    blanks = 0;

    if (playerMoney < 0)
    {
        game.removeChild(spinButton.getImage());
        spinButton = new objects.Button("assets/images/spinButton.png", 43, 470);
        game.addChild(spinButton.getImage());
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
    var betLine = [" ", " ", " "];
    var outCome = [0, 0, 0];

    for (var spin = 0; spin < 3; spin++) {
        outCome[spin] = Math.floor((Math.random() * 65) + 1);
        switch (outCome[spin]) {
            case checkRange(outCome[spin], 1, 27):  // 41.5% probability
                betLine[spin] = "minion";
                blanks++;
                break;
            case checkRange(outCome[spin], 28, 37): // 15.4% probability
                betLine[spin] = "red";
                grapes++;
                break;
            case checkRange(outCome[spin], 38, 46): // 13.8% probability
                betLine[spin] = "bomb";
                bananas++;
                break;
            case checkRange(outCome[spin], 47, 54): // 12.3% probability
                betLine[spin] = "hal";
                oranges++;
                break;
            case checkRange(outCome[spin], 55, 59): //  7.7% probability
                betLine[spin] = "chuck";
                cherries++;
                break;
            case checkRange(outCome[spin], 60, 62): //  4.6% probability
                betLine[spin] = "matilda";
                bars++;
                break;
            case checkRange(outCome[spin], 63, 64): //  3.1% probability
                betLine[spin] = "terence";
                bells++;
                break;
            case checkRange(outCome[spin], 65, 65): //  1.5% probability
                betLine[spin] = "blues";
                sevens++;
                break;
        }
    }
    return betLine;
}


/* This function calculates the player's winnings, if any */
function determineWinnings() {
    console.log("Reached" + grapes);
    if (blanks == 0) {
        if (grapes == 3) {
            winnings = playerBet * 10;
            
        }
        else if (bananas == 3) {
            winnings = playerBet * 20;
            bananas = 0;
        }
        else if (oranges == 3) {
            winnings = playerBet * 30;
            oranges = 0;
        }
        else if (cherries == 3) {
            winnings = playerBet * 40;
            cherries = 0;
        }
        else if (bars == 3) {
            winnings = playerBet * 50;
            bars = 0;
        }
        else if (bells == 3) {
            winnings = playerBet * 75;
            bells = 0;
        }
        else if (sevens == 3) {
            winnings = playerBet * 100;
            sevens = 0;
        }
        else if (grapes == 2) {
            winnings = playerBet * 2;
            grapes = 0;
        }
        else if (bananas == 2) {
            winnings = playerBet * 2;
            bananas = 0;
        }
        else if (oranges == 2) {
            winnings = playerBet * 3;
            oranges = 0;
        }
        else if (cherries == 2) {
            winnings = playerBet * 4;
            cherries = 0;
        }
        else if (bars == 2) {
            winnings = playerBet * 5;
            bars = 0;
        }
        else if (bells == 2) {
            winnings = playerBet * 10;
            bells = 0;
        }
        else if (sevens == 2) {
            winnings = playerBet * 20;
            sevens = 0;
        }
        else {
            winnings = playerBet * 1;
        }

        if (sevens == 1) {
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
    background = new createjs.Bitmap("assets/images/background.png");
    game.addChild(background);

    for (var index = 0; index < NUM_REELS; index++) {
        tileContainers[index] = new createjs.Container();
        tileContainers[index].x = 100 + (118 * index);
        tileContainers[index].y = 290;
        game.addChild(tileContainers[index]);
    }

    //postion winning text
    winningText.x = 330;
    winningText.y = 411;
    game.addChild(winningText);

    scoreText.x = 92;
    scoreText.y = 411;
    game.addChild(scoreText);

    pointsWonText.x = 213;
    pointsWonText.y = 411;
    game.addChild(pointsWonText);

    jackpotText.x = 108
    jackpotText.y = 120;
    game.addChild(jackpotText);

    // Spin Button
    spinButton = new objects.Button("assets/images/spinButton.png", 43, 470);
    game.addChild(spinButton.getImage());

    spinButton.getImage().addEventListener("click", spinReels);

    //power Button
    powerButton = new objects.Button("assets/images/offbtn.png", 417, 460);
    game.addChild(powerButton.getImage());

    // Reset Button
    resetButton = new objects.Button("assets/images/resetButton.png", 427, 550);
    game.addChild(resetButton.getImage());

    // Bet 10
    betTenButton = new objects.Button("assets/images/bet10.png", 180, 491);
    game.addChild(betTenButton.getImage());

    // Bet Max
    betMaxButton = new objects.Button("assets/images/betMax.png", 180, 550);
    game.addChild(betMaxButton.getImage());

    powerButton.getImage().addEventListener("click", function () {
        game.removeChild(powerButton.getImage());
        powerButton = new objects.Button("assets/images/onbtn.png", 417, 460);
        game.addChild(powerButton.getImage());
    });

    betTenButton.getImage().addEventListener("click", function () {
        playerBet = 10;
        game.removeChild(onOffText);
        onOffText = new createjs.Text("On", "23px play", "#2EFE2E");
        onOffText.x = 300
        onOffText.y = 491;
        game.addChild(onOffText);
    });

    betMaxButton.getImage().addEventListener("click", function () {
        playerBet = 20;
        game.removeChild(onOffText);
        onOffText = new createjs.Text("On", "23px play", "#2EFE2E");
        onOffText.x = 300
        onOffText.y = 550;
        game.addChild(onOffText);
    });
}



// Our Game Kicks off in here
function main() {
    // instantiate my game container
    game = new createjs.Container();
    game.x = 23;
    game.y = 6;

    // Create Slotmachine User Interface
    createUI();


    stage.addChild(game);
}