// remember! array indices start at 0

var colors = ['red', 'blue', 'yellow'];

// using array indices, write an expression that will evaluate to the color described by these variable names
// for example var theColorBlue = colors[1];
// uncomment the next two lines and replace ____ with the appropriate code
var theColorRed = colors[0];
var theColorBlue = colors[1];
var theColorYellow = colors[2];

// what happens if we access an index that has no element?
// uncomment the following line and replace ____ with the appropriate code
// var fortyThirdColor = color[43];

// Answer >> Uncaught ReferenceError: color is not defined
// at arrayIndexConsiderations.js:17. This apperars in the console because
// the element that is being called on does not exist.

function lastItem( array ) {
  // this function accepts an array as an argument
  // have it return the last item in the array
  return colors[2];
  //console.log(colors[2]);
}
lastItem(colors);