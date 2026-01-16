'use strict';
const playerEl0 = document.querySelector('.player--0');
const playerEl1 = document.querySelector('.player--1');
const score0 = document.querySelector('#score--0');
const score1 = document.querySelector('#score--1');
const player0 = document.querySelector('#current--0');
const player1 = document.querySelector('#current--1');
const diceR = document.querySelector('.dice');
const btnRoll = document.querySelector('.btn--roll');
const btnNew = document.querySelector('.btn--new');
const btnHold = document.querySelector('.btn--hold');
let score, activePlayer, currentScore, playing;
const init = function () {
  score = [0, 0];
  currentScore = 0;
  playing = true;
  document;
  playerEl0.classList.remove('player--winner');
  playerEl1.classList.remove('player--winner');
  activePlayer = 0;
  playerEl0.classList.add('player--active');
  playerEl1.classList.remove('player--active');
  diceR.classList.add('hidden');
  score0.textContent = 0;
  score1.textContent = 0;
  player0.textContent = 0;
  player1.textContent = 0;
};
init();
const switchPlayer = function () {
  document.getElementById(`current--${activePlayer}`).textContent = 0;
  currentScore = 0;
  activePlayer = activePlayer === 0 ? 1 : 0;
  playerEl0.classList.toggle('player--active');
  playerEl1.classList.toggle('player--active');
};

btnRoll.addEventListener('click', function () {
  if (playing) {
    //generating random number
    const dice = Math.trunc(Math.random() * 6) + 1;
    //diplay dice
    diceR.classList.remove('hidden');
    diceR.src = `dice-${dice}.png`;
    //adding the dice to the current score of the active player
    if (dice !== 1) {
      //add
      currentScore += dice;
      document.getElementById(`current--${activePlayer}`).textContent =
        currentScore;
    } else {
      //switch player
      switchPlayer();
    }
  }
});
btnHold.addEventListener('click', function () {
  if (playing) score[activePlayer] += currentScore;
  document.getElementById(`score--${activePlayer}`).textContent =
    score[activePlayer];
  if (score[activePlayer] >= 100) {
    playing = false;
    diceR.classList.add('hidden');
    document
      .querySelector(`.player--${activePlayer}`)
      .classList.add('player--winner');
    document
      .querySelector(`.player--${activePlayer}`)
      .classList.remove('player--active');
  } else switchPlayer();
});
btnNew.addEventListener('click', init);