// Functions
function nextSequence() {
  var randomNumber = Math.floor(Math.random() * 4);
  var randomChosenColor = buttonColors[randomNumber];
  gamePattern.push(randomChosenColor);
  buttonAnimation(randomChosenColor);
  playSound(randomChosenColor);
  $("#level-title").text("level " + level);

  level++;
  return randomChosenColor;
}

function playSound(id) {
  switch (id) {
    case "red":
      var redSound = new Audio("sounds/red.mp3");
      redSound.play()
      break;

    case "blue":
      var blueSound = new Audio("sounds/blue.mp3");
      blueSound.play()
      break;
    case "green":
      var greenSound = new Audio("sounds/green.mp3");
      greenSound.play()
      break;
    case "yellow":
      var yellowSound = new Audio("sounds/yellow.mp3");
      yellowSound.play()
      break;
  }
}

function clickHandler(id) {
  buttonAnimation(id);
  var userChosenColor = id;
  userClickedPattern.push(id);
  playSound(id);
  // console.log(gamePattern, userClickedPattern)

}

function buttonAnimation(currentKey) {
  var activeButton = document.querySelector('.' + currentKey);
  activeButton.classList.add('pressed');
  setTimeout(function() {
    activeButton.classList.remove('pressed')
  }, 100);
}

function checkAnswer(currentLevel) {
  for (i = 0; i < currentLevel; i++) {
    if (gamePattern[i] != userClickedPattern[i]) {
      $("body").addClass("game-over");
      setTimeout(function() {
        $("body").removeClass("game-over");
      }, 200);
      var wrongSound = new Audio("sounds/wrong.mp3");
      wrongSound.play();
      $("h1").text("Game over. Press any key to restart");
      startFlag = false;

      return false;
    }
  }

  setTimeout(function() {
    nextSequence();
  }, 1000);
  return true;
}


// Variables
buttonColors = ["red", "blue", "green", "yellow"];
var startFlag = false;
var level = 0;
var inputCount = 0;
var gamePattern = [];
var userClickedPattern = [];
// Action Detection
$(".btn").click(function() {
  if (startFlag == true) {

    clickHandler($(this).attr('id'));
    inputCount++;
    if (inputCount == gamePattern.length) {
      checkAnswer(level);
      userClickedPattern = [];
      inputCount = 0;
    }
  }
})

$(document).keypress(function(event) {
  if (startFlag == false) {
    startFlag = true;
    gamePattern = [];
    userClickedPattern = [];
    level = 0;
    inputCount = 0;
    nextSequence();
  }
});