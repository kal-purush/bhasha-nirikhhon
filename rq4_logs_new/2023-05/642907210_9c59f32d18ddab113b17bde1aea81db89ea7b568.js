// Define the task for a minion
const buyGroceries = (arrayOfFoodToBuy) => {
    // Code to buy groceries with money
    for (const food of arrayOfFoodToBuy) {
        console.log(`I bought ${food}`)
    }
}

// Tell the minion to perform the task with specific information
const items = ["Milk", "Onions", "Ketchup"]
items.push('Eggs', 'Grits')
buyGroceries(items)

const fillGasTank = (gallons) => {
    if (gallons > 15) {
        console.log('You cannot add this much gas to the tank')
    } else {
        console.log(`I filled the tank with ${gallons} of gas`)
    }
}

fillGasTank(16)

//Define a parameter that will hold the value of the original number.
const quarterValue = (value) => {

    // Divide that number by 4
    let quartered = value / 4
    // Return the quartered number
    return quartered
}
// Store the returned number in a variable
const result = quarterValue(16)

// Plug that variable into the parenthesis for the console.log()
console.log(result)
// When you run the code, you should see the answer in the console.
// Invoke the function and store the return value


// Log the return value
console.log()