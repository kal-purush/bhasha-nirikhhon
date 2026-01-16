function find(array, criteriaFn) {  
 let current = array   // current var represents the current array we are on to keep track
 let next = []    // next ver keeps track of the elements we have yet to go through
 while (current) {    // the while loop only executes if there is an element in current and if undefined we'll exit the loop
  if (criteriaFn(current)) {    // means if current satisfies the criteria
  return current     // then we retun it and exit the entire function
 }
 if (Array.isArray(current)) {  // if variable current is equal to an array
 for (let i = 0; i < current.length; i++) { // iterate through it's elements with a for loop
 next.push(current[i])  // and push all of them onto next
 }
}

current = next.shift() // make the first element of next the new current to be passed to the while loop

return null // if we haven't done this then return null


1) current var represents the current array we are on to keep track
2) next keeps track of the elements we have yet to go through
3) the while loop only executes if there is an element in current and if undefined we'll exit the loop
4) if (criteriaFn(current)) means if current equals to the criteria then we retun it and exit the function
5) if variable current is equal to an array iterate through it's elements with a for loop and push all of them onto next
6) after pushing any children of current onto next - if there are any children - we make the first element of next the new current to be the next to be passed to the while loop and if we haven't done this then return null 