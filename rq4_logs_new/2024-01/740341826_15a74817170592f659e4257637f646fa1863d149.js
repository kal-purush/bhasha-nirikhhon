//Game variables
const buttonColours = ["blue","red","green","yellow"];
var gamePattern = [];
var userClickedPattern = [];
var gameStarted = 0;
var level = 0;

//Start the game with any keypress
//disabled after game has been initialized
$(document).keypress(function(){
  //check if game has started
  if(gameStarted === 0){
    $("body").removeClass("game-over");
    level = 0;
    nextSequence();
    gameStarted = 1;
  }
});

//generate next level button
function nextSequence(){
  // generate random number between 0-3 to represent the color tiles.
  let randomNumber = Math.round(Math.random()* 3);
  const randomChosenColor = buttonColours[randomNumber];
  gamePattern.push(randomChosenColor);

  //display current level
  $("h1").text("Level " + level);
  level++;

  //LEVEL: button flash + animate + makesound
  $("." + randomChosenColor).flash(100,1);
  playSound(randomChosenColor);
  animatePress(randomChosenColor);
}

// flash button
$.fn.flash = function(duration, iterations) {
    duration = duration || 1000; // Default to 1 second
    iterations = iterations || 1; // Default to 1 iteration
    var iterationDuration = Math.floor(duration / iterations);

    for (var i = 0; i < iterations; i++) {
        this.fadeOut(iterationDuration).fadeIn(iterationDuration);
    }

    return this;
}

// user click button and add to answer array
$(".btn").click(function(){
  // add clicked button to user's chosen pattern
  const userChosenColor = $(this).attr("id");
  userClickedPattern.push(userChosenColor);

  playSound(userChosenColor);
  animatePress(userChosenColor);
  checkAnswer(userClickedPattern.length-1);
  // if user completes this level, generate next level.
  // Edge case: check whether game has been initialized, only generate level if game started
  if(userClickedPattern.length == gamePattern.length && gameStarted == 1){
    // give eye some response time before generating next level
    setTimeout(function(){
      // generate next level
      nextSequence();
      userClickedPattern = [];
    }, 1000);
  }
})

// play sound
function playSound(color){
    const audio = new Audio("/Users/rorobee/Desktop/WebDev/SimonGame/sounds/" + color + ".mp3");
    audio.play();
}

// animate pressed action
function animatePress(color){
  $("." + color).addClass("pressed");
  setTimeout(function(){
    $("." + color).removeClass("pressed");
  }, 50);
}

//check gamePattern vs userClickedPattern,
// if doesn't match, return gameOver
// if match, continue
function checkAnswer(i){
  if(gamePattern[i] != userClickedPattern[i]){
    console.log("fail");
    gameOver();
    return;
  }
  console.log("success");
}

// display gameover text, reset all variables, and end current game
function gameOver(){
  $("h1").text("Game Over Bitch, Press Any Key to Restart");
  playSound("wrong");
  gameStarted = 0;
  gamePattern = [];
  userClickedPattern = [];
  $("body").addClass("game-over");
}