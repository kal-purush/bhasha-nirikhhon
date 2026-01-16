'use strict';
const player0El = document.querySelector('.player--0');
const player1El = document.querySelector('.player--1');
const score0El = document.querySelector('#score--0');
const score1El = document.getElementById('score--1');
const current0El = document.getElementById('current--0');
const current1El = document.getElementById('current--1');

const diceEl = document.querySelector('.dice');
const btnNew = document.querySelector('.btn--new');
const btnRoll = document.querySelector('.btn--roll');
const btnHold = document.querySelector('.btn--hold');
score0El.textContent = 0;
score1El.textContent = 0;
let  activePlayer = 0 ; 
const score =[0,0];
let playing = true ;
// diceEl.classList.add('hidden');
const switchPlayer = function (activePlayer){
  if (activePlayer===0){
    player0El.classList.add('player--active');
    player1El.classList.remove('player--active');}
    else {
      player0El.classList.remove('player--active');
      player1El.classList.add('player--active');}
}
let curentScore = 0;
btnRoll.addEventListener('click', function () {
 if (playing){
  const dice = Math.trunc(Math.random() * 6) + 1;
  console.log('dice');
  
  diceEl.classList.remove('hidden');
  diceEl.src = `dice-${dice}.png`;
  if (dice !== 1) {
    curentScore += dice;
    document.getElementById(`current--${activePlayer}`
    ).textContent=curentScore
    switchPlayer(activePlayer);
  }
  else {
    document.getElementById(`current--${activePlayer}`
    ).textContent=0 ;
    curentScore = 0 ;
    activePlayer= activePlayer === 0 ? 1 : 0 ;
    switchPlayer(activePlayer);

    // player--1.classList.add('player--active');
  
    }
}});
btnHold.addEventListener('click',function(){
 if (playing){
  score[activePlayer] += curentScore
 curentScore= 0 ;
 document.getElementById(`current--${activePlayer}`
    ).textContent=curentScore
  document.getElementById(`score--${activePlayer}`
  ).textContent =score[activePlayer] ;
  if (score[activePlayer]>=10){
    playing = false ;
    document.querySelector(`.player--${activePlayer}`)
    .classList.add('player--winner');
    document.querySelector(`.player--${activePlayer}`)
    .classList.remove('player--active')
    diceEl.classList.add('hidden');

  }else{
  switchPlayer(activePlayer);
  activePlayer= activePlayer === 0 ? 1 : 0 ;
  }
}})
btnNew.addEventListener('click',function(){
  playing=true;
  document.querySelector(`.player--${activePlayer}`)
  .classList.remove('player--winner');
  document.querySelector(`.player--${activePlayer}`)
  .classList.add('player--active');

  curentScore= 0 ;
 document.getElementById(`current--${activePlayer}`
    ).textContent=curentScore
    score[0]=0 ;
    score[1]=0 ;
  document.getElementById(`score--0`
  ).textContent =score[activePlayer]  ;
  document.getElementById(`score--1`
  ).textContent =score[activePlayer]  ;
})