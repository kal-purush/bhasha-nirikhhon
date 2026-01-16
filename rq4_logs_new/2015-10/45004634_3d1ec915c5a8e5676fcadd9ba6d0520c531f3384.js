var userDeck = [];
var userPoints = 0;
var computerDeck = [];
var computerPoints = 0;
var cards = [{value: 1}, 
			 {value: 2}, 
			 {value: 3}, 
			 {value: 4}
			 ];



//this function assigns the shuffled array to either userDeck or computerDeck
var assignDeck = function(array){ 
	for(var i = 0; i < array.length; i++){
		if(i % 2 === 0){
			userDeck.push(array[i]);
		} else {
			computerDeck.push(array[i]);
		}; 
	};
};	
// http://stackoverflow.com/questions/2450954/how-to-randomize-shuffle-a-javascript-array
	//this is the website I got the shuffle funciton for arrays from

//This is a function used to shuffle the deck //Make start button, then have it change to flip button
var shuffle = function(userInput){      
	if(userInput === "startGame"){
		var shuffleArray = function(array) {
    	for (var i = array.length - 1; i > 0; i--) {
        	var j = Math.floor(Math.random() * (i + 1));
        	var temp = array[i];
        	array[i] = array[j];
        	array[j] = temp;
    		}
    		array = array;
    		return array;
		}
	shuffleArray(cards);
	} 
}
// This is a function to check to see if either the computer or player won the game
var winnerCheck = function (playerPoints, computerPoints) {
	if(playerPoints === 2){
		console.log("Congrats you won the game!");
		return false;
	} else if(computerPoints === 2) {
		console.log("You lose sucker!");	
		return false;
	} 
};

//This is the main function that starts the game and incorporates all above functions and variables
var mainGame = function(){            
	shuffle("startGame");
	assignDeck(cards); 

	console.log("userDeck: ", userDeck);
	console.log("computerDeck: ", computerDeck);

		for(var i = 0; i < 2; i++){                       //Do not hard code length
			if(userDeck[i].value > computerDeck[i].value){
				userPoints++;
				winnerCheck(userPoints, computerPoints);
			} else if (userDeck[i].value === computerDeck[i].value){
					if(userDeck[i + 1].value >= computerDeck[i + 1].value){
						userPoints = userPoints + 2;
						winnerCheck(userPoints, computerPoints);	
					} 
					else {
						computerPoints = computerPoints + 2;
						winnerCheck(userPoints, computerPoints);
					}
			} else {
				computerPoints++;
				winnerCheck(userPoints, computerPoints);
			}
		};
		console.log("UserPoints: ", userPoints);
		console.log("ComputerPoints:", computerPoints);

		
};

mainGame(); //Have method start when user click "start button, restart button"
