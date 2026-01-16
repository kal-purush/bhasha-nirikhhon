const readline = require('readline');

function isPrime(num) {
    if (num <= 1) return false;
    if (num <= 3) return true;
    if (num % 2 === 0 || num % 3 === 0) return false;
    for (let i = 5; i * i <= num; i += 6) {
        if (num % i === 0 || num % (i + 2) === 0) return false;
    }
    return true;
}

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

rl.question("Enter a number to check if it's a prime: ", (userInput) => {
    let numberToCheck = Number(userInput);

    if (!isNaN(numberToCheck) && Number.isInteger(numberToCheck)) {
        console.log(isPrime(numberToCheck) ? "The number is prime." : "The number is not prime.");
    } else {
        console.log("Invalid input. Please enter a valid integer.");
    }

    rl.close();
});