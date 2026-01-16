

// getComputerChoice function that randomly returns Rock, paper or scissors


let computerSelection
let randomNumber

function getRandomNumber () {
  randomNumber = Math.ceil(Math.random() * 3);
  return randomNumber;
}

console.log(randomNumber)

function getComputerChoice () {
  let randomNumber = Math.ceil(Math.random() * 3);
    if (randomNumber === 1) {
      let computerSelection = "rock";
    } else if (randomNumber === 2) {
      computerSelection = "paper"
    } else if (randomNumber === 3) {
      computerSelection = "scissors"
    }
   return (computerSelection,randomNumber);
}


console.log(getComputerChoice())
console.log(computerSelection)

// output with console.log
// function that plays a single round of RPS which takes two parameters: playerSelection and computerSelection and returns string declaring winner like so "You Lose! Paper beats Rock"
// Make your function’s playerSelection parameter case-insensitive (so users can input rock, ROCK, RocK or any other variation).



// function playRound(playerSelection, computerSelection) {
    // your code here!
//  }
//   
//  const playerSelection = "rock";
//  const computerSelection = getComputerChoice();
//  console.log(playRound(playerSelection, computerSelection));


  // Write a NEW function called game(). Use the previous function inside of this one to play a 5 round game that keeps score and reports a winner or loser at the end.