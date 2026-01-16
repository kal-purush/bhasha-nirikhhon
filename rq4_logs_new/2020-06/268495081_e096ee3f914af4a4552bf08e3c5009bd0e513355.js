// DOM selector for the page transition
const landingPageBtn = document.querySelector(".landing-page__btn");
const landingPage = document.querySelector(".landing-page");
const gamePage = document.querySelector(".game-page");

// transition from landingPage to gamePage
landingPageBtn.addEventListener("click", () => {
  landingPage.classList.add("fadeIn");
  gamePage.classList.remove("fadeIn");
  gamePage.classList.add("fadeOut");
});

// player choice btn
const rockBtn = document.querySelector(".player-choice__rock-btn");
const paperBtn = document.querySelector(".player-choice__paper-btn");
const scissorsBtn = document.querySelector(".player-choice__scissors-btn");
const playerBtn = document.querySelectorAll(".player-choice>div");

// hands pictures
const rock = "./Images/rock.png";
const paper = "./Images/paper.png";
const scissors = "./Images/scissors.png";
const handArray = [rock, paper, scissors];

// player / computer Hands 
const playerHand = document.querySelector(".playground__player-hands");
const computerHand = document.querySelector(".playground__computer-hands");

// hand animation removal
const removeAnimationClass = () => {
  playerHand.classList.remove("playerHandShaker");
  computerHand.classList.remove("computerHandShaker");
};

// hand animation toggle
const handAnimation = () => {
  playerHand.classList.add("playerHandShaker");
  computerHand.classList.add("computerHandShaker");
  setTimeout(removeAnimationClass, 1500);
};


// function for randomising computer hand choice 
const getRandomInt = e => (Math.floor(Math.random() * Math.floor(e)));

// scoreboard and result message
const playerScore = document.querySelector(".scoreboard__player--score");
const computerScore = document.querySelector(".scoreboard__computer--score");
const resultMessage = document.querySelector(".result-message");
const winningMessage = ["You won !", "Good job ;)", "Well Played"];
const losingMessage = ["You lost :'(", "Bad luck", "Try again ..."];



// game mechanic
const gameMechanic = (playerHandResult, computerHandResult) => {
  if (playerHandResult === computerHandResult) {
    resultMessage.textContent = "it's a tie";
  } else if (playerHandResult === rock) {
    if (computerHandResult === scissors) {
      resultMessage.textContent = winningMessage[getRandomInt(3)];
      playerScore.textContent++;
    } else if (computerHandResult === paper) {
      resultMessage.textContent = losingMessage[getRandomInt(3)];
      computerScore.textContent++;
    }
  } else if (playerHandResult === paper) {
    if (computerHandResult === rock) {
      resultMessage.textContent = winningMessage[getRandomInt(3)];
      playerScore.textContent++;
    } else if (computerHandResult === scissors) {
      resultMessage.textContent = losingMessage[getRandomInt(3)];
      computerScore.textContent++;
    }
  } else if (playerHandResult === scissors) {
    if (computerHandResult === paper) {
      resultMessage.textContent = winningMessage[getRandomInt(3)];
      playerScore.textContent++;
    } else if (computerHandResult === rock) {
      resultMessage.textContent = losingMessage[getRandomInt(3)];
      computerScore.textContent++;
    }
  }
};

//match playerHand with playerBtn "Clicked"
const getPlayerHandToMatchBtn = (btnClicked) => {
  if (btnClicked === "Rock") return playerHand.setAttribute("src", handArray[0]);
  if (btnClicked === "Paper") return playerHand.setAttribute("src", handArray[1]);
  if (btnClicked === "Scissors") return playerHand.setAttribute("src", handArray[2]);
};

// handSwitcher
const handSwitcher = (btnClicked) => {
  getPlayerHandToMatchBtn(btnClicked);
  computerHand.setAttribute("src", handArray[getRandomInt(3)]);
};

// regrouped all function that needed to be timedOut
const handResultHandling = (btnClicked) => {
  handSwitcher(btnClicked);
  gameMechanic(playerHand.attributes.src.value, computerHand.attributes.src.value);
  addEventListenerList(playerBtn, "click", gameStart);
};

// addEventListener loop for Array
function addEventListenerList(list, event, fn) {
  for (let item of list) {
    item.addEventListener(event, fn, false);
  };
};

// remove addEventListener Loop for Array => prevent multiple clicking 
function removeEventListenerList(list, event, fn) {
  for (let item of list) {
    item.removeEventListener(event, fn, false);
  };
};

// same eventListener for all playerBtn starting the game function
addEventListenerList(playerBtn, "click", gameStart);

// game function regrouping all logique : running when a playerBtn is "clicked"
function gameStart(e) {
  removeEventListenerList(playerBtn, "click", gameStart);
  handAnimation();
  setTimeout(handResultHandling, 1500, e.target.textContent);
};





/* working gameStart without timeOut
function gameStart(e) {
  handAnimation();
  handSwitcher(e.target.textContent);
  gameMechanic(playerHand.attributes.src.value, computerHand.attributes.src.value);

};
*/