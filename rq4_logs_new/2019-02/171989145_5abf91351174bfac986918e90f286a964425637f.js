var caughtCounter = 0;
var fledCounter = 0;
var guessesCounter = 10;
var lettersGuessed = [];

var chosenWord = ""; 
var playerGuess = [];

var wordList = ["pikachu", "bulbasaur", "charizard", "togepi", "snorlax"];

var writeToPage = function() {
  document.getElementById("caught").textContent = caughtCounter;
  document.getElementById("fled").textContent = fledCounter;
  document.getElementById("remaining").textContent = guessesCounter;
  document.getElementById("guessed").textContent = lettersGuessed;
  document.getElementById("result").textContent = playerGuess;
};

var checkLetter = function(letter) {
  var letterInWord = false;

  for(var i = 0; i < chosenWord.length; i++) {
    if (letter === chosenWord[i]) {
      letterInWord = true;
      playerGuess[i] = chosenWord[i];
    }
  }
};

var gameOver = function(letter) {
  checkLetter(letter);

  guessesCounter--;

if (guessesCounter + lettersGuessed === chosenWord.length) {
      caughtCounter++;
      alert("You Won");
    startGame();
  }
else if (guessesCounter === 0) {
    fledCounter++;
    alert('You lost');
    startGame();
  }
 
  writeToPage();
};

document.onkeyup = function(event) {
  if (guessesCounter > 0) {

    
    if (event.keyCode >= 65 && event.keyCode <= 90) {
      var letter = event.key.toLowerCase();

      lettersGuessed.push(letter);

      gameOver(letter);
    }
  }
};

var startGame = function() {
  caughtCounter = 0;
  fledCounter = 0;
  guessesCounter = 10;
  lettersGuessed = [];
  chosenWord = "";

  var randomNumber = Math.floor(Math.random() * wordList.length);
  chosenWord = wordList[randomNumber];

  for(var i = 0; i < chosenWord.length; i++) {
    playerGuess.push("_");
  }

  writeToPage();
};

startGame();