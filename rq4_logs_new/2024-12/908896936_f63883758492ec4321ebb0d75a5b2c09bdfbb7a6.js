function foo(a, b) {
  a = a === null ? 0 : a; 
  b = b === null ? 0 : b; 
  return a + b; 
}
console.log(foo(1, null)); //Output: 1
console.log(foo(1, 2)); //Output: 3
console.log(foo(null, null)); //Output: 0