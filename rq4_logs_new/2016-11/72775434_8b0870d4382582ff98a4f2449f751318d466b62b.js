var planets = ["mercury", "venus", "earth", "mars", "jupiter", "saturn", "uranus", "neptune"];

/*
 Use the forEach method to add the name of each planet
 to a div element in your HTML with an id of "planets".
*/
var el = document.getElementById("planets");


function injectPlanets(array){
	el.innerHTML += "<h4>" + array + "</h4>";
	// console.log("El", el);
};

planets.forEach(injectPlanets);


// Use the map method to create a new array where the first letter of each planet is capitalized
function capitalize(array){
	return array.charAt(0).toUpperCase() + array.slice(1, array.length)
	//console.log("El", el);
}


var bigLetter = planets.map(capitalize);
//console.log("", bigLetter);

function sift(array){
	if(array.)
}

// Use the filter method to create a new array that contains planets with the letter 'e'


// Use the reduce method to create a sentence from the words in the following array

var words = ["The", "early", "bird", "might", "get", "the", "worm", "but", "the", "second", "mouse", "gets", "the", "cheese"];