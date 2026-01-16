var X;
var makeAButton = true; 
function setup(){
	createCanvas(600,600);
	x=0;
}

function draw(){
	background(0);
	rectMode(CENTER);
	rect(x,250,100,100);
	var
	x+= D;
	if (x>width/2 && makeAButton){
		myButton = document.createElement("button");
		myButton.textContent = "Change Direction!";
		$("body").append(myButton);
		keepGoing = false;
		function changeIt(){

		}
	}
}