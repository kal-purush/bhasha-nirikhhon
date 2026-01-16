// OBJECTS

var myCat = {
	tail: "long",
	age: 29,
	color: "blue",
	friends: ["zoe", "callan", "lauren"]
}

console.log("age of cat: ", myCat.age);
console.log("color of cats: ", myCat["color"]);

for(var myKey in myCat){
	console.log("myKey", myKey);
	console.log("values", myCat[myKey]);
}