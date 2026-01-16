//Part 2 of the Assignment. This function will use a prompt box to receive a name argument and print response.

function myFunction() {
    var person = prompt("Please enter your name");
    if (person != null) {
        document.getElementById("name").innerHTML =
        "You entered the name, " + person  + " Thanks for participating!";
    }
}

// Part 3 of the Assignment.  This simple function will print to the console without an argument.

function newFunction() {
	console.log("This will print in the console");
}
newFunction()
