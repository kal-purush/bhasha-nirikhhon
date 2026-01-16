var computerChoice =  ["a","b","c","d","e","f","g","h","i","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","z"];
var win = 0;
var lose= 0;
var guesses = 0;
var guessesLeft = 9;
var guessedLetters = [];

var computerGuess = computerChoice[Math.floor(Math.random() * computerChoice.length)]; //why doesn't this work when inside the onkey function?

document.onkeyup = function(event) {
      var userGuess = event.key; 

  // adds letter guessed to end of lettersGuessed array
      guessedLetters.push(userGuess);  

    
      console.log(userGuess);
      console.log(computerGuess);

// if user guesses, subtract one from guesses left
  if(userGuess) {
      guessesLeft--;
      document.getElementById("guesses").innerHTML = guessesLeft;
    }

// if user guess is same as random computer guess, then increase point. else if out of guesses, add to losses. 
    if(userGuess === computerGuess) {
     	win++
      document.getElementById("win").innerHTML = win;
    } else if(guessesLeft === 0) {
     lose++
     document.getElementById("loss").innerHTML = lose;
 	}

// why does this have to before reset function?
if (guessesLeft === 0) {
	reset ();
}
document.getElementById("guessedLetters").innerHTML = guessedLetters;
}


var reset = function () {
	if (guessesLeft === 0){
		guesses = 0;
		guessesLeft = 9;
		guessedLetters = [];
    document.getElementById("guessesLeft").innerHTML = guesses;
    document.getElementById("guessedLetters").innerHTML = guessedLetters;
	}


 
    };