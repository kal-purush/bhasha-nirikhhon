"use strict";

// Selecting HTML elements
let score0 = document.getElementById("score--0");
let score1 = document.getElementById("score--1");
const diceImage = document.querySelector(".dice");
const diceRollBtn = document.querySelector(".btn--roll");

// Setting up initial game state
score0.textContent = 0;
score1.textContent = 0;
diceImage.classList.add("hidden");

let randomNum = 0;

// Functions declaration
const diceRoll = function (diceNum) {
  diceNum = Math.trunc(Math.random() * 6 + 1);
  return diceNum;
};

diceRollBtn.addEventListener("click", function () {
  randomNum = diceRoll();
  console.log(typeof randomNum, randomNum);
  if (randomNum === 1) {
    diceImage.src = "dice-1.png";
    diceImage.classList.remove("hidden");
    score0 += randomNum;
    score0.innerHTML = score0;
    score1 += randomNum;
    score1.innerHTML = score1;
  } else if (randomNum === 2) {
    diceImage.src = "dice-2.png";
    diceImage.classList.remove("hidden");
    score0 += randomNum;
    score0.innerHTML = score0;
    score1 += randomNum;
    score1.innerHTML = score1;
  } else if (randomNum === 3) {
    diceImage.src = "dice-3.png";
    diceImage.classList.remove("hidden");
    score0 += randomNum;
    score0.innerHTML = score0;
    score1 += randomNum;
    score1.innerHTML = score1;
  } else if (randomNum === 4) {
    diceImage.src = "dice-4.png";
    diceImage.classList.remove("hidden");
    score0 += randomNum;
    score0.innerHTML = score0;
    score1 += randomNum;
    score1.innerHTML = score1;
  } else if (randomNum === 5) {
    diceImage.src = "dice-5.png";
    diceImage.classList.remove("hidden");
    score0 += randomNum;
    score0.innerHTML = score0;
    score1 += randomNum;
    score1.innerHTML = score1;
  } else if (randomNum === 6) {
    diceImage.src = "dice-6.png";
    diceImage.classList.remove("hidden");
    score0 += randomNum;
    score0.innerHTML = score0;
    score1 += randomNum;
    score1.innerHTML = score1;
  }
});