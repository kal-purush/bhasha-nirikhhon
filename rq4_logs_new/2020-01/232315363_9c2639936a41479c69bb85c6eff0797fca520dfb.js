const buttons = document.querySelectorAll('button');
const currentScore = document.createElement('p');
const scoreDiv = document.querySelector('.score');
const playerWinsText = "You win the round!";
const computerWinsText = "Computer wins the round!";
const results = document.querySelector('.results');

let playerScore = 0;
let computerScore = 0;
let playerChoice;


buttons.forEach((button) => {
  button.addEventListener('click', (e) => {
    playerChoice = button.className;
    playRound(playerChoice, computerPlay());
    showCurrentScore();
    if (playerScore == 5 || computerScore == 5) {
      showResults();
    } 
  })
})

function computerPlay() {
  number = Math.floor((Math.random() * 3) + 1);
  if (number == 1) {
    return "rock";
  } else if (number == 2) {
    return "paper";
  } else if (number == 3) {
    return "scissors";
  }
}

function playRound(playerChoice, computerChoice) {
  let roundResults = document.createElement('p');
  roundResults.textContent = `Computer chooses ${computerChoice}. `;
  if (playerChoice === computerChoice) {
    roundResults.textContent += "This round is a draw!";
  } else if (playerChoice === "rock") {
    if (computerChoice === "paper") {
      computerScore++;
      roundResults.textContent += computerWinsText;
    } else if (computerChoice === "scissors") {
      playerScore++;
      roundResults.textContent += playerWinsText;
    }
  } else if (playerChoice === "paper") {
    if (computerChoice === "rock") {
      playerScore++;
      roundResults.textContent += playerWinsText;
    } else if (computerChoice === "scissors") {
      computerScore++;
      roundResults.textContent += computerWinsText;
    }
  } else if (playerChoice === "scissors") {
    if (computerChoice === "rock") {
      computerScore++;
      roundResults.textContent += computerWinsText;
    } else if (computerChoice === "paper") {
      playerScore++;
      roundResults.textContent += playerWinsText;
    }
  }
  results.appendChild(roundResults);
}

function showCurrentScore() {
  currentScore.textContent = `You: ${playerScore}, Computer: ${computerScore}`;
  scoreDiv.appendChild(currentScore);
}

function showResults() {
  if (playerScore >= 5) {
    alert("Game over, You win!");
  } else if (computerScore >= 5) {
    alert("Game over, You suck! Computer wins!");
  }
  location.reload();
}

function resetGame() {
  playerScore = 0;
  computerScore = 0;
}