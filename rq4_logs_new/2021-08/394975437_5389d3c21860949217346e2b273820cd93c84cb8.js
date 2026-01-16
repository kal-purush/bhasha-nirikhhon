var buttonColors = ["green","red","yellow","blue"];
var gamePattern=[];
var userClickedPattern=[];

var level = 0;
var started = false;

$(document).keypress(function(event) {
	if(!started){		
		started = true;
		nextSequence();
	}
});

$(".btn").click(function(event){
	var userColorChosen = $(this).attr('id');
	playSound(userColorChosen);
	animatePress(userColorChosen);
	userClickedPattern.push(userColorChosen);
	check(userClickedPattern.length-1);
});

function check(pos) {
	if(userClickedPattern[pos] == gamePattern[pos] ){
  	if(userClickedPattern.length == gamePattern.length){ // user has successfully completed current level;
   		setTimeout(function(){
   			nextSequence();
   		},1000);
   	}
  }
  else{                            // user has failed this level, so reset all values to initial i.e start of game
  	startOver();
  }
}

function startOver() {
	level = 0;
	started = false;
	gamePattern = [];
	userClickedPattern = [];

	$("#level-title").text('Game Over, Press Any Key to Restart');
	$("body").addClass('game-over');
	playSound("wrong");
	setTimeout(()=>{ $("body").removeClass('game-over')}, 200);
}

function nextSequence() {
	userClickedPattern = [];

	level++;
	var levelinfo = document.querySelector("#level-title").innerHTML = "Level " + level;

	randomNumber = Math.round(Math.random()*3);
	var randomChosenColor = buttonColors[randomNumber];
	gamePattern.push(randomChosenColor);

	$("#"+randomChosenColor).fadeIn(100).fadeOut(100).fadeIn(100);
	playSound(randomChosenColor);
}


function playSound(name){
	var audio = new Audio("sounds/"+name+".mp3");
	audio.play();
}

function animatePress(currentColor){
	$("#"+currentColor).addClass('pressed');
	setTimeout(() => { $("#"+currentColor).removeClass('pressed');}, 100);
}