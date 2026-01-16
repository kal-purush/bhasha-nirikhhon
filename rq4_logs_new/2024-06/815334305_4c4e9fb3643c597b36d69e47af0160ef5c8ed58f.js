let generatedNumbers = [];

function generateRandomNumber() {
    const min = 11111;
    const max = 99999;
    const randomNumber = Math.floor(Math.random() * (max - min + 1)) + min;

    if (!generatedNumbers.includes(randomNumber)) {
        generatedNumbers.push(randomNumber);
        document.getElementById('randomNumber').innerText = "REPL" + randomNumber;
    } else {
        // If the number already exists, generate a new one
        generateRandomNumber();
    }
}