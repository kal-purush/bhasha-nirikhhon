/* 
 FUNCTIONAL JAVASCRIPT IS GOOD
───────────────────────────────
 Basic: Reduce
 Exercise 6 of 18


# Task

Given an Array of strings, use Array#reduce to create an object that contains the number of times each string occured in the array. Return the object directly (no need to console.log).

## Example

    var inputWords = ['Apple', 'Banana', 'Apple', 'Durian', 'Durian', 'Durian']
    
    console.log(countWords(inputWords))
    
    // =>
    // {
    //   Apple: 2,
    //   Banana: 1,
    //   Durian: 3
    // }

## Arguments

  * inputWords: An array of random Strings.

## Conditions

  * Do not use any for/while loops or Array#forEach.
  * Do not create any unecessary functions e.g. helpers.

## Resources

  * https://en.wikipedia.org/wiki/Reduce_(higher-order_function)
  * https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/reduce

## Boilerplate

    function countWords(inputWords) {
      // SOLUTION GOES HERE
    }
    
    module.exports = countWords

*/

module.exports = function (inputWords) {
	return inputWords.reduce(function (countObj, currentWord, index, arr) {
		countObj[currentWord] = countObj[currentWord] || 0; 
		countObj[currentWord] += 1; 

		return countObj;  
	}, {})
}; 

/* 
input:      [ 'nulla', 'irure', 'mollit', 'lorem', 'duis', 'irure', 'incididunt', 'sit', 'commodo', 'dolor', 'reprehenderit', 'eu', 'quis', 'culpa', 'laboris', 'ad', 'elit', 'esse', 'eu', 'enim', 'consequat', 'lorem', 'sit', 'quis', 'cupidatat', 'do', 'est', 'aute', 'aute', 'aliquip', 'enim', 'excepteur', 'exercitation', 'minim', 'aliquip', 'consectetur' ]
submission: { nulla: 1, irure: 2, mollit: 1, lorem: 2, duis: 1, incididunt: 1, sit: 2, commodo: 1, dolor: 1, reprehenderit: 1, eu: 2, quis: 2, culpa: 1, laboris: 1, ad: 1, elit: 1, esse: 1, enim: 2, consequat: 1, cupidatat: 1, do: 1, est: 1, aute: 2, aliquip: 2, excepteur: 1, exercitation: 1, minim: 1, consectetur: 1 }
solution:   { nulla: 1, irure: 2, mollit: 1, lorem: 2, duis: 1, incididunt: 1, sit: 2, commodo: 1, dolor: 1, reprehenderit: 1, eu: 2, quis: 2, culpa: 1, laboris: 1, ad: 1, elit: 1, esse: 1, enim: 2, consequat: 1, cupidatat: 1, do: 1, est: 1, aute: 2, aliquip: 2, excepteur: 1, exercitation: 1, minim: 1, consectetur: 1 }

# PASS

Your solution to Basic: Reduce passed!

Here's the official solution in case you want to compare notes:

────────────────────────────────────────────────────────────────────────────────
    function countWords(arr) {
      return arr.reduce(function(countMap, word) {
        countMap[word] = ++countMap[word] || 1 // increment or initialize to 1
        return countMap
      }, {}) // second argument to reduce initialises countMap to {}
    }
    
    module.exports = countWords

────────────────────────────────────────────────────────────────────────────────

*/