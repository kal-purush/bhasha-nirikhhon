// # Callback Exercises
//
//
// ## Question One
//
// Write a function that takes in a callback and calls the callback
//
// ```
// Input:
// function() { console.log("Hello!") }
//
// Output:
// Logs "Hello!" to the console
// ```
console.log("Quesion 1: ");
const callbackFunction1 = (callback) => {
  callback();
}
const function1 = () => {
  console.log("Hello!");
}
callbackFunction1(function1);
console.log("--------------------------------------");
// ## Question Two
//
// Write a function that takes in a callback and a boolean.  If the boolean is true, call the callback, otherwise log "Ignoring the callback" to the console.
//
// ```
// Input:
// () => console.log("")
//
// ```
console.log("Quesion 2: ");
const callbackFunction2 = (callback, bool) => {
  if(bool === true) {
    console.log("The callback has been called");
  } else {
    console.log("Ignoring callback");
  }
}
const function2 = () => {

}
callbackFunction2(function2, true);
console.log("--------------------------------------");
// ## Question Three
//
// Write a function that take in a number and two callbacks.  It should apply the first callback to the number, then the second callback, then return the result
//
// ```
// Input: 4, (num) => num + 3, (num) => num * 2
//
// Output: 14
//
// ```
console.log("Quesion 3: ");
const callbackFunction3 = (num, callback1, callback2) => {
  console.log("Input: " + num + ", => ");
  callback2(callback1(num));
}
const function3A = (num) => {
  console.log(`${num}` + " + 3 =>")
  return num += 3;
}
const function3B = (num) => {
  console.log(`${num}` + " * 2")
  return console.log(num *= 2)
}
callbackFunction3(5, function3A, function3B);
console.log("--------------------------------------");
// ## Question Four
//
// You are given the following functions:
// ```js
//
// function conservativeSpender(balance)  {
//   return balance > 100
// }
//
// function liberalSpender(balance) {
//   return balance > 10
// }
//
// function horribleSaver (balance) {
//   return balance > 0
// }
//
// ```
// Write a function `shouldIBuyThis` that takes in a balance and a callback (one of the above functions). The function should return either
// "Sure! I've got the money!" or "Nope! Gotta keep my savings up!"
//
// ```js
// console.log(shouldIBuyThis(20, horribleSaver))
// // logs: "Sure! I've got the money!"
// console.log(shouldIBuyThis(20, liberalSpender))
// // logs: "Sure! I've got the money!"
// console.log(shouldIBuyThis(20, conservativeSpender))
// // logs:  "Nope! Gotta keep my savings up!"
// console.log(shouldIBuyThis(101, conservativeSpender))
// // logs: "Sure! I've got the money!"
//
// ```
console.log("Quesion 4: ");
const conservativeSpender = (balance) => {
  console.log("Nope! Gotta keep my savings up!")
  return balance > 100
}
const liberalSpender = (balance) => {
  console.log("Sure! I've got the money!")
  return balance > 10
}
const horribleSaver = (balance) => {
  console.log("Sure! I've got the money!")
  return balance > 0
}
const shouldIBuyThis = (balance, Callback) => {
  if(balance >= 100) {
    conservativeSpender(balance);
  } else if(balance > 10 && balance < 100) {
    liberalSpender(balance);
  } else if(balance > 0 && balance <= 10) {
    horribleSaver(balance);
  }
}
console.log("--------------------------------------");
// ## Question Five
//
// a. Write a function `forEachElem` that takes in an array and a callback. Call the callback on each element in the array.
//
// ```
// Input: [1,4,5,6], (elem) => console.log("$" + elem)
//
// Output:
// Logs the following to the console:
// 1
// 4
// 5
// 6
// ```
console.log("Quesion 5: ");
const forEachElem = (arrayInput, callback) => {
  for(let i = 0; i < arrayInput.length; i++) {
    callback(arrayInput[i]);
  }
}
const callbackFunction5 = (element) => {
  console.log("$" + element);
}
let array5 = [2, 4, 5, 6];
forEachElem(array5, callbackFunction5);
// b. Create a function `logEachElem` that takes an array as an argument logs every element in the input array.  Use `forEachElem` inside the body of your function.
//
//
// ```
// Input: [1,2,34]
//
// Output:
// Logs the following to the console:
// 1
// 2
// 34
// ```
let array5b = [1, 2, 3, 4, 5];
const logEachElem = (arrayInput, callback) => {
  forEachElem(array5b, callbackFunction5b);
}
const callbackFunction5b = (element) => {
  console.log(element)
}
logEachElem(array5b, forEachElem)
// c. Create a function `logArrTypes` that uses the `forEachElem` method to log each array element followed by its type.
//
//
// ```
// Input: [1,"Hi", true, {color: "green"}]
//
// Output:
// Logs the following to the console:
// 1 is a number
// Hi is a string
// true is a boolean
// [object Object] is a object
// ```
const logArrTypes = (arrayInput, callback) => {
  forEachElem(arrayInput, callback)
}
const callbackFunction5c = (element) => {
  console.log(element + " is a " + typeof(element) )
}
let array5c = ["a", 1, false, { }];
logArrTypes(array5c, callbackFunction5c);
console.log("--------------------------------------");
// ## Question Six
//
// a. Write a function called `myMap` that takes in an array and a callback as arguments.  It should return a new array with the callback applied to each element in the array.
//
// ```
// Input: [3,5,6], function(num) { return num * 3 }
//
// Output: [9, 15, 18]
// ```
console.log("Quesion 6: ");
const myMap = (arrayInput, callback) => {
  callback(arrayInput)
}
const callbackFunction6a = (arrayInput) => {
  let newArray = [];

  for(let i = 0; i < arrayInput.length; i++) {
    newArray.push(arrayInput[i]);
  }
  for(let i = 0; i < newArray.length; i++) {
    newArray[i] *= 3;
  }
  console.log(JSON.stringify(newArray));
}
let array6a = [3, 5, 6];
myMap(array6a, callbackFunction6a);
//
// b. Create a function `allCaps` that takes in an array as an argument, and uses the `myMap` function to return all the elements capitalized.
//
// ```
// Input: ["The", "internet", "is", "not", "a", "big", "truck.", "It's", "a", "series", "of", "tubes"]
//
// Output:
//
// [
//     'THE', 'INTERNET',
//      'IS',      'NOT',
//       'A',      'BIG',
//  'TRUCK.',     "IT'S",
//       'A',   'SERIES',
//      'OF',    'TUBES'
// ]
// ```
const allCaps = (arrayInput, callback) => {
  for(let i = 0; i < arrayInput.length; i++) {
    arrayInput[i] = arrayInput[i].toUpperCase();
  }
  console.log(JSON.stringify(arrayInput));
}
let capsArray = ["The", "internet", "is", "not", "a", "big", "truck.", "It's", "a", "series", "of", "tubes"];
allCaps(capsArray, myMap);
console.log("--------------------------------------");
// ## Question Seven
//
// a. Write a function called `myFilter` that takes in an array and a callback as arguments.  The callback should take in an element and return a boolean.  `myFilter` should return a new array with only values that return true when passed into the callback.
//
// ```
// Input: [1,4,6,7, -3], num => num < 5
//
// Output:  [1, 4, -3]
// ```
console.log("Quesion 7: ");
const myFilter = (arrayInput, callback) => {
  let filteredArray = [];
  callback(arrayInput)
  for(let i = 0; i < arrayInput.length; i++) {
    if(callback(arrayInput[i]) === true) {
      filteredArray.push(arrayInput[i])
    }
  }
  console.log(JSON.stringify(filteredArray));
}
const callbackFunction7a = (element) => {

  if(element < 5) {
    return true;
  }
}
let array7a = [1, 4, 6, 7, -3];
myFilter(array7a, callbackFunction7a);
// b. Write a function called `removeEvenNumbers` that takes in an array and returns an array with all the even numbers removed.
//
// ```
// Input: [1,2,3,4,5,6,7,8,9]
// Output: [1,3,5,7,9]
// ```
const removeEvenNumbers = (arrayInput) => {
  let newArray = [];
  for(let i = 0; i < arrayInput.length; i++) {
    if(arrayInput[i] % 2 !== 0) {
      newArray.push(arrayInput[i])
    }
  }
  console.log(JSON.stringify(newArray));
}
let array7b = [1, 2, 3, 4, 5, 6, 7, 8, 9];
removeEvenNumbers(array7b);
// c. Write a function called `removeShortWords` that takes in an array and returns an array with all strings containing fewer than 4 characters removed.
//
// ```
// Input: ["JavaScript", "is", "a", "high-level,", "interpreted", "programming", "language", "that", "conforms", "to", "the", "ECMAScript", "specification"]
//
// Output:
// [ 'JavaScript',
//   'high-level,',
//   'interpreted',
//   'programming',
//   'language',
//   'that',
//   'conforms',
//   'ECMAScript',
//   'specification' ]
// ```
const removeShortWords = (arrayInput) => {
  let newArray = [];
  for(let i = 0; i < arrayInput.length; i++) {
    if(arrayInput[i].length >= 4) {
      newArray.push(arrayInput[i]);
    }
  }
  console.log(JSON.stringify(newArray));
}
let array7c = ["JavaScript", "is", "a", "high-level,", "interpreted", "programming", "language", "that", "conforms", "to", "the", "ECMAScript", "specification"];
removeShortWords(array7c);