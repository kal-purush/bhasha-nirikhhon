// Create JavaScript code that loops through an array of numbers and returns a new array with only the odd numbers from the original array. 
// If there is only one odd number in the array, return an array with that single value.

// create a variable for the input array
const input = [2, 4, 6, 8, 11, 20, 15, 22];
// const input = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
// const input = [70, 42, 55, 81, 21, 91, 34];
// const input = [2, 4, 6, 8, 10, 11, 12];
// create an empty array for the output
let output = [];

// for loop to iterate all items in input array
for (let i = 0; i < input.length; i++) {
  // if current item is odd
  if (input[i] % 2 === 1) {
    // then add it to the output array
    output.push(input[i]);
  }
}

console.log(output);