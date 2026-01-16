var level = 1;

// var level will help us to keep track of which level is the current player.

var started = false;

// var started will help us to know if the game already started, if false, the game has not started, if true, the game already started.

var userClickedPattern = [];

// var userClickedPattern array will keep track how what buttons the player already pushed.

var gamePattern = [];

// var gamePattern arrray will help us to keep track of what colors were generated randomly in nextSequence() function, this will help us 1st
// to keep track, 2nd to compare with var userClickedPattern and 3rd to repeat each of the stored colors everytime the user push correct buttons.

var buttonColors = ["red", "blue", "green", "yellow"];

// var buttonColors are the colors of each button, that will help us to generate random selected colors.

$(document).keydown(function(){
    if (started === false){
        nextSequence();
        started = !started;
    } else {
        console.log("The game has already started!")
    }
});

$("h1").click(function(){
    if (started === false){
        nextSequence();
        started = !started;
    } else {
        console.log("The game has already started!")
    }
});

// when a keydown is detected, the webpage will apply the following, first, the var started is false, so it will call nextSequence() function and apply started = !started, that means started will pass from false to true. If a keydown is again detected, the game  will remain the same because started is already true, so it will pass to the console "The game has already started!".

$(".btn").click(function(){
    if (started === false){
        console.log("Game has not started yet. Keydown not detected.")
    } else {
        var userChosenColor = $(this).attr("id");
        userClickedPattern.push(userChosenColor);
        
        animatePress(userChosenColor);
        
        playSound(userChosenColor);
        
        checkAnswer(userClickedPattern.length - 1);
    };
});

// when a button is clicked, it will check if the game has already started, if not, it will console.log "Game has not started yet. Keydown not detected." If started = true, the game will execute the following, first get the color of the clicked button and store it in the var userClickedPattern, as said, to keep track of it. Then it will animate the click over the button and also play a sound, each of the colors have different sounds, so everytime it click a different color, it will reproduce their specific sound. And last, it will call the checkAnswer() fucntion, to verify if the user selected the correct color sequence, comparing var userClickedPattern and var gamePattern.

function nextSequence(){
    var randomNumber = Math.round(Math.random() * 3);
    
    var randomChosenColor = buttonColors[randomNumber];
    gamePattern.push(randomChosenColor);

    $("#level-title").text("Level " + level);

    var gamesQty = 0;

    function myLoop(){
        setTimeout(function(){
            $("#" + gamePattern[gamesQty]).fadeOut(100).fadeIn(100);
            playSound(gamePattern[gamesQty]);
            gamesQty++;
            if(gamesQty < gamePattern.length){
                myLoop();
            }
        }, 500);
    };

    myLoop();

    level++;

    userClickedPattern = [];

    $("#level-reached").text("");
};

// The nextSequence() function is almost where all the magic happens, here, first, the function will generate a random number and it will help us to selected a random color inside var buttonColors array and store it inside the var gamePattern.

// The text of the webpage title will change to "Level" + level, the current number of var level is equal to 1, so it will show 1 once nextSequence() function is called.

//Then theres the var gamesQty, that will help us to run the next myLoop() function, inside our nextSequence() function. myLoop() function will help us to repeat every color that var gamePatterns has stored inside it, so it will remember the player what colors have already been called. 

//Inside myLoop() we have a setTimeOut() function will help us to have a delay everytime a color is repeated to the user so it will be more easy to remember. Inside it, it will make every button stored in gamePattern to have a fadeout and reproduce their specific sounds, once executed, it will add +1 to var gamesQty, with the help of var gameQty it will be possible to select every index inside var gamePattern to repeat them.

//If the number stored in var gamesQty is less than the var gamePattern length, it will execute myLoop() again, this is very helpful to repeat every color stored inside var gamePattern, once the number of var gamesQty is equal or bigger than var gamePattern length, the function will stop.

//At the end, the nextSequence() function will add +1 to var level, increasing it by 1 everytime nextSequence() function is called, and resetting var userClickedPattern array to and empty array, this will help because we need the player to repeat all the colors everytime the the level change.


function playSound(name){
    var audio = new Audio ("./sounds/"+ name + ".mp3");
    audio.play();
};

// The playSound() function will help us to reproduce a sound everytime the nextSequence() function generates a new random color and when the player click a button.

function gameOverSound(){
    var audio = new Audio ("./sounds/wrong.mp3");
    audio.play();
}

//Every time the user selects an incorrect color or fails the sequence, it will reproduce a wrong sound.

function animatePress(currentColor){
    $("#" + currentColor).addClass("pressed");

    setTimeout(function(){
        $("#" + currentColor).removeClass("pressed");
    }, 100);
};

//This function changes the CSS of the clicked button.

function startOver(){
    level = 1;
    gamePattern = [];
    userClickedPattern = [];
    started = false;
}

//This function resets the game to original values.

function checkAnswer(currentLevel){
    if (gamePattern[currentLevel] === userClickedPattern[currentLevel]){
        console.log("success");

        if (userClickedPattern.length === gamePattern.length){
            setTimeout(function () {
              nextSequence();
            }, 1000);
          };

    } else {
        console.log("wrong");

        level--;

        $("#level-title").text("Game Over, Press Here or Any Key to Restart.");
        $("#level-reached").text("Level Reached: " + level);

        $("body").addClass("game-over");
        setTimeout(function(){
            $("body").removeClass("game-over");
        }, 200);

        gameOverSound();

        startOver();
    };
};

//And our last function, the checkAnswer() function, first it will check and compare indexes between gamePattern and userClickedPattern using level as an input, if the last color generated by the game and the last color selected by the player are the same, it will console.log "success" but, thats not all, to call nextSequence() function (that is what keeps running our game), it also need to met another requirement, the userClickedPattern length MUST be exactly the same as gamePattern length, this is obviously because this means that the player repeated the same pattern as gamePattern stored colors.

//If none of the above requeriments is met, then else is where the player is heading, where it console.log "wrong", it shows in the webpage title that the game is over, also creating a flashing red background, reproducing the gameOverSound() function and calling startOver() function, that resets all the progress already made.