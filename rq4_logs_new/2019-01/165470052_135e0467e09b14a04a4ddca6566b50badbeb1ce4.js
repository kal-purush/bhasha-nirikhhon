
var userInput = document.getElementById("name-input");

window.alert("Script active ")
/*
	Basic test requirments
		* Has too be not an empty string
		* has t0 be a a length of 7 char, for now

*/
function testIfValid(userInput){
	if(userInput = ""){
		return false;
	}
	if(userInput.length != 7){
		return false;
	}
}

function printResult(){
	if (testIfValid(userInput) == false){
		window.alert("Not valid")
	}
}