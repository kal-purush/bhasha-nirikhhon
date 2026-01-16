let userScore = 0;
let computerScore = 0;
const userScore_span = document.getElementById('user-score');
const computerScore_span = document.getElementById('computer-score');
const scoreBoard_div = document.querySelector('.score-board');
const result_p = document.querySelector('.result > p');
const rock_div = document.getElementById('r');
const paper_div = document.getElementById('p');
const scissors_div = document.getElementById('s');

function getComputerChoice() {
  const choices = ['r', 'p', 's'];
  const randomNumber = Math.floor(Math.random() * choices.length);
  return choices[randomNumber];
}

function convertToWord(letter) {
  if (letter == 'r') return 'Rock';
  if (letter == 'p') return 'Paper';
  return 'Scissors';
}

function win(userChoice, computerChoice) {
  userScore++;
  userScore_span.innerHTML = userScore;

  const smallUserWord = 'user'.fontsize(3).sub();
  const smallCompWord = 'comp'.fontsize(3).sub();
  result_p.innerHTML =
    convertToWord(userChoice) + smallUserWord +
    ' beats ' + convertToWord(computerChoice) + smallCompWord + '. ' +
    'You win!';

  const userChoice_div = document.getElementById(userChoice);
  userChoice_div.classList.add('green-glow');
  setTimeout(
    () => userChoice_div.classList.remove('green-glow'),
    300
  );
}

function lose(userChoice, computerChoice) {
  computerScore++;
  computerScore_span.innerHTML = computerScore;

  const smallUserWord = 'user'.fontsize(3).sub();
  const smallCompWord = 'comp'.fontsize(3).sub();
  result_p.innerHTML =
    convertToWord(userChoice) + smallUserWord +
    ' loses to ' + convertToWord(computerChoice) + smallCompWord + '. ' +
    'You lost...';

  const userChoice_div = document.getElementById(userChoice);
  userChoice_div.classList.add('red-glow');
  setTimeout(
    () => userChoice_div.classList.remove('red-glow'),
    300
  );
}

function draw(userChoice, computerChoice) {
  const smallUserWord = 'user'.fontsize(3).sub();
  const smallCompWord = 'comp'.fontsize(3).sub();
  result_p.innerHTML =
    convertToWord(userChoice) + smallUserWord +
    ' is equal to ' + convertToWord(computerChoice) + smallCompWord + '. ' +
    "It's a draw.";

  const userChoice_div = document.getElementById(userChoice);
  userChoice_div.classList.add('gray-glow');
  setTimeout(
    () => userChoice_div.classList.remove('gray-glow'),
    300
  );
}

function game(userChoice) {
  const computerChoice = getComputerChoice();

  if (userChoice == computerChoice) return draw(userChoice, computerChoice);

  switch (userChoice + computerChoice) {
    case 'pr':
    case 'rs':
    case 'sp':
      return win(userChoice, computerChoice);
  }

  lose(userChoice, computerChoice);
}

function main() {
  rock_div.addEventListener('click', () => game('r'));
  paper_div.addEventListener('click', () => game('p'));
  scissors_div.addEventListener('click', () => game('s'));
}

main();