var numChildren = Math.floor(Math.random()*10);
var partnerName = "Nick";
var geographicLocation = "San Francisco";
var jobTitle = "Architect";

var fortune = "You will be a" + " " + jobTitle + " " + "in" + " " + geographicLocation + " " + "and married to" + " " + partnerName + " " +"with" + " " + numChildren + " " + "kids."
console.log(fortune);

function ageCalc(born, future) {
	var age = "You are" + " " + (future - born) + " " + "years old."
	console.log(age)
}
ageCalc(1998,2017);