function getComputerChoice() {
  let choices = ['ROCK', 'PAPER', 'SCISSORS'];
  return choices[Math.floor(Math.random() * choices.length)];
}

function playRound(playerSelection, computerSelection) {
  if (playerSelection === 'ROCK' && computerSelection === 'PAPER') {
    console.log("Player: ROCK. Computer: PAPER. You lose!");
    return -1;
  }
  else if (playerSelection === 'ROCK' && computerSelection === 'SCISSORS') {
    console.log("Player: ROCK. Computer: SCISSORS. You win!");
    return 1;
  }
  else if (playerSelection === 'PAPER' && computerSelection === 'ROCK') {
    console.log("Player: PAPER. Computer: ROCK. You win!");
    return 1;
  }
  else if (playerSelection === 'PAPER' && computerSelection === 'SCISSORS') {
    console.log("Player: PAPER. Computer: SCISSORS. You lose!");
    return -1;
  }
  else if (playerSelection === 'SCISSORS' && computerSelection === 'ROCK') {
    console.log("Player: SCISSORS. Computer: ROCK. You lose!");
    return -1;
  }
  else if (playerSelection === 'SCISSORS' && computerSelection === 'PAPER') {
    console.log("Player: SCISSORS. Computer: PAPER. You win!");
    return 1;
  }
  else if (playerSelection === computerSelection) {
    console.log(`Player: ${playerSelection}. Computer: ${computerSelection}. Draw!`);
    return 0;
  }
}

function game() {
  let validGames = 0;
  let playerScore = 0;
  let computerScore = 0;

  while (validGames < 5) {
    let playerInput = prompt("Rock, Paper, or Scissors?").toUpperCase();
    if (playerInput === 'ROCK' || playerInput === 'PAPER' || playerInput === 'SCISSORS') {
      let roundResult = playRound(playerInput, getComputerChoice());
      if (roundResult === 1) {
        playerScore++;
      }
      else if (roundResult === -1) {
        computerScore++;
      }
      console.log(`${playerScore}-${computerScore}`);
      validGames++;
    }
    else {
      console.log("Invalid input");
    }
  }
  
  let gameResult;
  if (playerScore > computerScore) {
    gameResult = "Player";
  }
  else if (computerScore > playerScore) {
    gameResult = "Computer"
  }
  else if (playerScore === computerScore) {
    gameResult = "Draw"
  }

  console.log(`Winner: ${gameResult}`)
}

game();