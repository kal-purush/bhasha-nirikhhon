var h3 = document.querySelector("h3");
var color1 = document.querySelector(".color1");
var color2 = document.querySelector(".color2");
var body = document.getElementById("gradient");
// var initialColor1 = document.querySelector(".color1.value") -- wrong
// var initialColor2 = document.querySelector(".color2.value") -- wrong

body.style.background = "linear-gradient(to right, " 
	+ color1.value 
	+ ", " 
	+ color2.value 
	+ ")";

console.log(body.style.background);

h3.textContent = body.style.background

function setGradient() {
	body.style.background = 
	"linear-gradient(to right, " 
	+ color1.value 
	+ ", " 
	+ color2.value 
	+ ")";

	h3.textContent = body.style.background + ";";
}

color1.addEventListener("input", setGradient);

color2.addEventListener("input", setGradient);


// --------------- before creating function setGradient ------------------------------

// color1.addEventListener("input", function() { 
// 	body.style.background = 
// 	"linear-gradient(to right, " 
// 		+ color1.value 
// 		+ ", " 
// 		+ color2.value 
// 		+ ")";
// })

// color2.addEventListener("input", function() {
// 	body.style.background = 
// 		"linear-gradient(to right, " 
// 		+ color1.value 
// 		+ ", " 
// 		+ color2.value 
// 		+ ")";
// })