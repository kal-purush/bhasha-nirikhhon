// JAVASCRIPT HIGHER ORDER FUNCTION 12/5/22

//BUILT-IN METHODS - premade functions that some developers made that are eaily invokable. Some take that information, some don't.

// console.log() - shows the results of the code being ran

// console.log(true.toUpperCase())
//output: 
//TypeError: true.toUpperCase is not a function
//key to this error is TypeError, calling something on a incorrect. Function cannot work on a boolean when it is a string method.

//What is a Function?
// A code machin, custom set of instructions/code that performs a task and returns some type of data
//default is to return undefined

// Function don't need names. Nameless functions are called Anonymous fxns

// Anonymous Function:
//  () => {
//     code goes here
//  }

// What is a parameter?
// placeholer for the data being passed into the fxn

// What is an argument?
// the actual data that gets passed in

// *** way to remember

// -P parameter- placeholer
// -A argument- actual data

// What is iteration?
// Set of instructions executed over and over until a condition is met
//- arrays
//-strings

// Syntactical Sugar - when developers wrap up large concepts into easy to use SyntaxError; makes heavy-lifting code functionality "sweeter to use"

//Higher Order Function (HOF)

// Higher Order Function is a fxn that takes a function as an argument
// - used by calling on built-in methods
// - more efficient that for loops
// - but use in very specific circumstances

//  ARRAY method
// - .forEach()
// - .map()
// - .filter()

const showArray = ["Wednesday", "Survivor", "Wheel of Fortune", "This Old House", "Severance", "Westworld"]

const numsArray = [5, 8, 15, 36, 42]

// - .forEach()
// wil execute something once per item in the array
//because it is a HOF it is going to take an anonymous function as an argument

    showArray.forEach(() => {
        console.log("Hey Hotel!")
    })
    //output:
    // "Hey Hotel!"
    // "Hey Hotel!"
    // "Hey Hotel!"
    // "Hey Hotel!"
    // "Hey Hotel!"
    // "Hey Hotel!"

//anonymous function takes an argument - required parameter is "value"
// - value is the value of the element at the current index in iteration process
// value is just a parameter can say anything  (v, val, element)

    showArray.forEach((value) => {
        console.log(`You like the show ${value}`)
    })

    //output:
    // "You like the show Wednesday"
    // "You like the show Survivor"
    // "You like the show Wheel of Fortune"
    // "You like the show This Old House"
    // "You like the show Severance"
    // "You like the show Westworld"

// - index - gives us access to the integer that represents the value's index (OPTIONAL)

    showArray.forEach((value, index) => {
        console.log(`You like the show ${value}. It's at position ${index}`)
    })
    //output:
    // You like the show Wednesday. It's at position 0
    // You like the show Survivor. It's at position 1
    // You like the show Wheel of Fortune. It's at position 2
    // You like the show This Old House. It's at position 3
    // You like the show Severance. It's at position 4
    // You like the show Westworld. It's at position 5

    showArray.forEach((value, index, array) => {
        console.log(`You like the show ${value}. It's at space ${index} in the array ${array}`)
    })

// - array - this array you're calling the HOF on (in this case, showArray) OPTIONAL

    showArray.forEach((turtle, cat, horse) => {
        console.log(`You like the show ${turtle}. It's at space ${cat} in the array ${horse}`)
    })

    showArray.forEach((value, _, array) => {
        console.log(`You like the show ${value}, in the array ${array}`)
    })

//THE ORDER WILL ALWAYS BE value, index, array - names can be anything - they are placeholders

// .forEach() doesn't return anthing. You can only get the values by console loging.

// .map()
// - similar to forEach, more useful 
// - transforming or displaying each value in array
// - difference is it iterates each element in a arry but it will return a new array of the same length
// original array is untouched, must assign may equation to a new variable because it is a accessor

const threesArray = [3, 6, 9, 12, 15]

    const multiplyBy2 = threesArray.map((value) => {
        return value * 2
    })
    console.log(multiplyBy2) 
    //this is not a function so we don't have to pass it anything

    //output: [ 6, 12, 18, 24, 30 ]

//using map in a dynamic function (reuseable - can pass any array of numbers into it)

// const threesArray = [3, 6, 9, 12, 15]
let numsArray1 = [3, 7, 89, 2, 14]

    const newFunction = (dom) => {
        return dom.map((value) => {
            if(value % 2 === 0) {
                return value * 2
            } else {
                return "nope"
            }
        })
    }

    console.log(newFunction(threesArray))
    //output: [ 'nope', 12, 'nope', 24, 'nope' ]

    const newerFunction = (arrayOfNumbers) => {
        return arrayOfNumbers.map((value) => {
            if(value % 2 === 0) {
                return value * 2
            } else {
                return "nope"
            }
        })
    }

    console.log(newerFunction(numsArray1))
    //output: [ 'nope', 'nope', 'nope', 4, 28 ]

//When you're only using one line of code you don't need {} or a return
// if only one value we don't need ()

//     const multiply - threesArray(value => {
//         return value * 2
//     })

// // SAME AS 

//     const multiplyBy - threesArray.map(value => value * 2)

// FILTER 
// .filter() - filters an array based on some condition we give it 
// - returns a new array but subset of the original array
// - built in if statement 

// - TRUTHY AND FALSE

// const showArray = ["Wednesday", "Survivor", "Wheel of Fortune", "This Old House", "Severance", "Westworld"]

    const searchW = showArray.filter((value) => {
        return value.toUpperCase().includes("W")
    })

    const search = showArray.filter((value) => {
        return value.includes("w") || value.includes("W")
    })

    console.log(searchW)
    //output: [ 'Wednesday', 'Wheel of Fortune', 'Westworld' ]

const mixedDataArray = [3, "hi", true, 50, 4, null, "Hotel", 42, false, "yo", 15]

//creats a fxn name onlyOdds
// parameter is an array - mixed data types
// use filter to iterate
//inside the fxn we'll creat a new variable oneType - placeholder of the filter by data type number
// filter over oneTypr to get the odds
// return the final variable

// typeof returns a string indicating the type of operator's value

    console.log(typeof 42) // output: number

    const onlyOdds = (array) => {
        let oneType = array.filter(value => {
            return typeof value === "number"
        })
        return oneType
        // console.log("^^^", oneType) //output: ^^^ [ 3, 50, 4, 42, 15 ]
    }
    console.log(onlyOdds(mixedDataArray))


    const onlyOdd = (array) => {
        let oneType = array.filter(value => {
            return typeof value === "number"
        })
        let odds = oneType.filter(value => {
            return value % 2 !==0
        })
        return odds
    }
    console.log(onlyOdd(mixedDataArray)) //output: [ 3, 15 ]