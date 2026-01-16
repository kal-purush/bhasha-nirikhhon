//  access the HTML elements using the id of the elements
var curWord = document.getElementById("currentWord");
var lettersUsed = document.getElementById("lettersGuessed");
var countGuess = document.getElementById("numOfGuesses");
var gameMessage = document.getElementById("userMessage");
var displayMessage = document.getElementById("dispMessage");
var playAudio = document.getElementById("audioFrame");
var audioFile = document.getElementById("h2O");
var pickWord = document.getElementById("nextWord");
		
// Draw the heading on the canvas
var drawCanvas = document.getElementById("imgCanvas");
if (drawCanvas.getContext) {
	var ctx = drawCanvas.getContext('2d');
	// Create gradient
	var grd = ctx.createLinearGradient(0,0,350,0);
	grd.addColorStop(0,"red");
	grd.addColorStop(1,"white");

	ctx.font = "20px Arial";
	ctx.fillStyle = grd;
	ctx.fillText("Lets Play!!!", 50, 20);
}	
		
// Declare variables for wins, remaining guesses, user choice
var remainingGuess = 12;
var wordChoice;
var pick;
var pos;
var userChoice;
var stage = 0;
var newGame = true;
		
countGuess.innerHTML = "<strong>"+remainingGuess+"</strong>";
		
// Declare arrays for word list and letters guessed by user
var wordList = ["Awkward", "Bagpipes", "Crypt", "Dwarves", "Fishhook", "Gazebo", "Ivory", "Jukebox", "Kayak", "Pixel", "Zombie", "Sphinx", "Jinx", "Quip", "Mystify"];
var lettersGuessed = [];
var selectedWord;
var displayedWord = [];

// This function uses the Math.random function to pick a random word from the word list array,
// loop through the selected word and dynamically create the blank spaces on the document equal to the word length.
 function pickAWord() {
 	resetVariables();
 	pick = Math.floor(Math.random() * wordList.length);
 	wordChoice = wordList[pick].toLowerCase();
 	pos = wordList.indexOf(wordList[pick]);
 	wordList.splice(pos, 1);
 	countGuess.innerHTML = "<strong>"+remainingGuess+"</strong>";
 	selectedWord = Array.from(wordChoice);
 	selectedWord.forEach(function(){
 		displayedWord.push("-");
 	});
 	curWord.innerHTML = "<span>" + displayedWord + "</span>";
 }

// The onkeyup function captures the user input
 document.onkeyup = function(event) {
	 if(newGame) {	
		audioFile.pause();
	 	gameMessage.innerHTML = "";
	 	userChoice = (String.fromCharCode(event.keyCode)).toLowerCase();
	 	
	 	hangman();
	 	
	 	if(lettersGuessed.indexOf(userChoice) === -1) {
	      	lettersGuessed.push(userChoice);
	 	    lettersUsed.innerHTML = "<strong>"+lettersGuessed+"<strong>";
	 	    //checkUserInput();
	 	}else {
	 	    displayMessage.innerHTML = "<strong>Letter already input before. Choose another letter.</strong>";	
	 	}	
	 }	
 }	

 // This function checks the user input against the selected word and replaces 
 // the letter in the user display at the right position if it matches.
 // It plays an audio if the word is guessed correctly by the player.
 function checkUserInput() {
 	for(var j=0; j<selectedWord.length; j++) {
 		if(selectedWord[j] === userChoice){
 			displayedWord.splice(j, 1, userChoice);
 		}
 	}
	
 	curWord.innerHTML = "<span>" + displayedWord + "</span>";
 	if(JSON.stringify(selectedWord) === JSON.stringify(displayedWord)) {
 		newGame = false;
 		gameMessage.innerHTML = "<strong>You got it!</strong>";
 		audioFile.src = "assets/audio/djlang59_-_Drops_of_H2O_(_The_Filtered_Water_Treatment_).mp3";
       	audioFile.type = "mp3";
       	audioFile.play();
       	//pickAWord();
     }
 }

// Event listener for the next word button
 pickWord.addEventListener('click', function() {
 	gameMessage.innerHTML = "";
 	newGame = true;
 	audioFile.pause();
 	pickAWord();
 });

// Function to reset all variables to initial state
function resetVariables() {
 	lettersGuessed.length = 0;
  	displayedWord.length = 0;
  	lettersUsed.innerHTML = "";
  	displayMessage.innerHTML = "";
 	remainingGuess = 12;
 	stage = 0;
 	ctx.clearRect(0, 0, drawCanvas.width, drawCanvas.height);
 	ctx.fillText("Lets Play!!!", 50, 20);
}

// Function calls the draw function for each wrong guess
function hangman() {
	if(selectedWord.indexOf(userChoice) === -1 && lettersGuessed.indexOf(userChoice) === -1) {
 		draw(stage);
 		stage++;
 		remainingGuess--;
 		countGuess.innerHTML = "<strong>"+remainingGuess+"</strong>";
 		if(stage === 12) {
 			newGame = false;
 			gameMessage.innerHTML = "<strong>Time up! Try Again.</strong>";
 	    	audioFile.src = "assets/audio/crazy-making-loopy-creature.wav";
 	    	audioFile.type = "audio/wav"
 	    	audioFile.play();
 	    	//pickAWord();
 		}
 	} else {
 		checkUserInput();
  	}
}

// Function draws the hangman image in steps each time user makes a wrong guess
function draw(step) {
	ctx.beginPath();
	if(step === 0) {
		ctx.moveTo(40,350);
		ctx.lineTo(40,30);
		ctx.stroke();
	} else if(step === 1) {
		ctx.moveTo(40,30);
		ctx.lineTo(175,30);
		ctx.stroke();
	} else if(step === 2) {
		ctx.moveTo(110,30);
    	ctx.lineTo(110,60);
    	ctx.stroke();
	} else if(step === 3) {
		ctx.arc(110, 110, 50, 0, Math.PI * 2, true); // Outer circle
		ctx.stroke();
	} else if(step === 4) {	
		ctx.moveTo(145, 110);
		ctx.arc(110, 110, 35, 0, Math.PI, false);  // Mouth (clockwise)
		ctx.stroke();
	} else if(step === 5) {
		ctx.moveTo(100, 100);
		ctx.arc(95, 100, 5, 0, Math.PI * 2, true);  // Left eye
		ctx.stroke();
	}else if(step === 6) {	
		ctx.moveTo(130, 100);
		ctx.arc(125, 100, 5, 0, Math.PI * 2, true);  // Right eye
		ctx.stroke();
	}else if(step === 7) {
		ctx.moveTo(110, 160);
		ctx.lineTo(110, 300);
		ctx.stroke();
	} else if(step === 8) {
		ctx.moveTo(110, 180);
		ctx.lineTo(60,200);
		ctx.stroke();
	}else if(step === 9) {
		ctx.moveTo(110, 180);
		ctx.lineTo(160,200);
		ctx.stroke();
	}else if(step === 10) {
		ctx.moveTo(110, 300);
		ctx.lineTo(60,350);
		ctx.stroke();
	}else if(step === 11) {
		ctx.moveTo(110, 300);
		ctx.lineTo(160,350);
		ctx.stroke();
	}
}		
