function add(a, b) {
  return Number(a) + Number(b);
}

function subtract(a, b) {
  return Number(a) - Number(b);
}

console.log(add(5, 3)); // Output: 8
console.log(subtract(5, 3)); // Output: 2
console.log(subtract(5, -3)); // Output: 8
console.log(subtract(-5, 3)); // Output: -8
//This is a simple example but it can be very difficult to debug when dealing with complex programs
//The solution is to explicitly convert the inputs to numbers using the Number() function before performing the arithmetic operation.
//This ensures that the operation is performed on numerical values, preventing unexpected type coercion behavior.