console.log("Hello world!");

// -------------------------------------------------------------------
// const computer = ["rock", "paper", "scissors"];
// const human = ["rock", "paper", "scissors"];

// console.log(Math.floor(Math.random()*computer.length));

// console.log(computer[Math.floor(Math.random()*computer.length)]);
// -------------------------------------------------------------------

// const choices = ["rock", "paper", "scissors"];

function getComputerChoice() {
    const choices = ["rock", "paper", "scissors"];
    return choices[Math.floor(Math.random()*choices.length)];
}

console.log(getComputerChoice());

function getHumanChoice() {
    let choice = prompt("Choose between: rock, paper or scissors: ");
    return choice.toLowerCase();
}

console.log(getHumanChoice());