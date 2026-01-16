
// primitive loop. works, but there are better ways
// prints out the values 0 through 9 
var x : number = 0;

while (x < 10) {
  console.log(x);
  x++;
}


// asdf
// asdf

let players : number[] = [3, 10, 4, 5, 1];

// FOR IN
// iterates and simply provides the index values for whatever collection we're going in
console.log("For/In");
for (let player in players) {
  console.log(player);
}

// FOR OF
// actually gives us the values
console.log("For/Of");
for (let player of players) {
  console.log(player);
}


// functions

// a function is a programming mechanism that allows you to encapsulate data behavior things like that 
// and then call it any time and have the function run whatever processes are inside of it.
function fullName(first, last) {
    return first + " " + last;
}



function printAddress(street: string, streetTwo: string) {
    console.log(street);
    if (streetTwo) {
        console.log(streetTwo);
    }
}
printAddress('123 Any St');


// setting default arguments
function printAddress(street: string, streetTwo?: string, state = 'AZ') {
    console.log(street);
    if (streetTwo) {
      console.log(state);
    }
}
printAddress('123 Any St');
printAddress('123 Any St', 'Suite 540');

// setting default arguments
function printAddress(street: string, streetTwo?: string, state = 'AZ') {
    console.log(street);
    if (streetTwo) {
      console.log(state);
    }
}
printAddress('123 Any St');
printAddress('123 Any St', 'Suite 540');
printAddress('123 Any St', 'Suite 540', 'UT');