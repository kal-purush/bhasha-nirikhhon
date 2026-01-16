var botScore=0,
	playerScore=0;

document.getElementById("rock").onclick=playerThrowsRock;
document.getElementById("scissors").onclick=playerThrowsScissors;
document.getElementById("paper").onclick=playerThrowsPaper;

function playerThrowsRock(){
	var botsWeapon = getRandomWeapon();
	checkWhoWon(botsWeapon,"rock");
}

function playerThrowsScissors(){
	var botsWeapon = getRandomWeapon();
	checkWhoWon(botsWeapon,"scissors");
}

function playerThrowsPaper(){
	var botsWeapon = getRandomWeapon();
	checkWhoWon(botsWeapon,"paper");
}

function getRandomWeapon(){
	var randomNumber=Math.random();
	var botsWeapon="rock";
	if(randomNumber<.33){
		botsWeapon="scissors";
	}
	else if(randomNumber<.6666){
		botsWeapon="paper";
	}
	return botsWeapon;
}
function checkWhoWon(botsWeapon,playersWeapon){
	if(botsWeapon==playersWeapon){
		displayCompleteMessage("There was tie");
	}
	else if(
		(botsWeapon=="scissors" && playersWeapon=="paper") ||
		(botsWeapon=="paper" && playersWeapon=="rock") ||
		(botsWeapon=="rock" && playersWeapon=="scissors")
		){
		increaseBotScore();
	}
	else{
		increasePlayerScore();
	}
}
function increaseBotScore(){
	botScore+=1;
	document.getElementById("computerScore").innerHTML=botScore;
	displayCompleteMessage("Sorry, you're a loser");
}
function increasePlayerScore(){
	playerScore+=1;
	document.getElementById("humanScore").innerHTML=playerScore;
	displayCompleteMessage("Whee!");
}
function displayCompleteMessage(msg){
	document.getElementById("status").innerHTML=msg;
}