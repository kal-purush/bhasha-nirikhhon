// register event handlers for the Change Button
function start()
{
	document.getElementById("changeButton").addEventListener("click", changeGreeting, false); 
}

// A JavaScript code that changes the HTML code that prints "Hello World" to have it say "Good Night World" 
function changeGreeting()
{
 		document.getElementById("greeting").innerHTML = "Good Night World";
}

// Call start function to register event handlers when the page is loading 
window.addEventListener("load", start, false); 	