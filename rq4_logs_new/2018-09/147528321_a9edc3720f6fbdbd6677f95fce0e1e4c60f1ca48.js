
/*
 * Global Variables
 */
const deck = document.querySelector('.deck');
let time = 0;
let clockOff = true;
let moves = 0;
let toggledCards = [];
let clockId;
let matched = 0;
let timerOff = false;


/*Timer and Clock
 functions */

 function displayTime()  {
   const clock = document.querySelector('.clock');
   const minutes = Math.floor(time / 60);
   const seconds = time % 60;
   if (seconds < 10) {
     clock.innerHTML = `${minutes}:0${seconds}`;
   } else {
     clock.innerHTML = `${minutes}:${seconds}`;
   }
     //console.log(clock);
 }

 //clock stop
  function stopClock()  {
      clearInterval(clockId);
  }

//clock start
 function startClock()  {
   let clockId = setInterval(() =>  {
     time++;
     displayTime();
     if (timerOff === true) {
       clearInterval(clockId);
     }
   }, 1000);
 }




 //End game
 function checkWin()  {
 const Total_pairs = 8;
 if (matched === Total_pairs)  {
     gameOver();
 }
 }

 function gameOver()  {
     //stopClock();
     toggleModal();
     writeModalStats();
     timerOff = true;
 }

 function replayGame()  {
   resetGame();
   toggleModal();
   resetCards();
 }


//Reset elements
 function resetCards()  {
   const cards= document.querySelectorAll('.deck li');
   for (let card of cards)  {
       card.className = 'card';
   }
 }

function resetGame()  {
  resetClockAndTime();
  resetMoves();
  resetStars();
  shuffleDeck();
  console.log('game is reset');
}

function resetClockAndTime()  {
  stopClock();
  clockOff = true;
  timerOff = false;
  time = 0;
  displayTime();
}

function resetMoves()  {
    moves = 0;
    document.querySelector('.moves').innerHTML = moves;
}

function resetStars()  {
    stars = 0;
    const starList = document.querySelectorAll('.stars li');
    for (star of starList)  {
      star.style.display = 'inline';
    }
}



//display stars
function getStars()  {
  stars = document.querySelectorAll('.stars li');
  starCount = 0;
  for (star of stars)  {
    if (star.style.display !== 'none')  {
        starCount++;
    }
  }
  console.log(starCount);
  return starCount;
}

//Modal
function toggleModal()  {
    const modal = document.querySelector('.modal__background');
    modal.classList.toggle('hide');
}
toggleModal();  //open modal
toggleModal();  //hide modal

function writeModalStats()  {
    const timeStat = document.querySelector('.modal__time');
    const clockTime = document.querySelector('.clock').innerHTML;
    const movesStat = document.querySelector('.modal__moves');
    const starsStat = document.querySelector('.modal__stars');
    const stars = getStars();
    timeStat.innerHTML = `Time = ${clockTime}`;
    movesStat.innerHTML = `Moves = ${moves}`;
    starsStat.innerHTML = `Stars = ${stars}`;
}

//Cancel button
document.querySelector('.modal__cancel').addEventListener('click', () =>  {
  toggleModal();
});

//Replay button
document.querySelector('.modal__replay').addEventListener('click', replayGame);

//Reset button
document.querySelector('.restart').addEventListener('click', resetGame);



function addMove()  {
  moves++;
  const movesText = document.querySelector('.moves');
  movesText.innerHTML = moves;
}


//Hide star
function checkScore()  {
  if (moves === 16 || moves === 24
) {   hideStar();
    }
}

function hideStar()  {
  const starList = document.querySelectorAll('.stars li');
  for (star of starList)  {
    if(star.style.display != 'none')  {
      star.style.display = 'none';
      break;
    }
  }
}


/*
 * Display the cards on the page
 *   - shuffle the list of cards using the provided "shuffle" method below
 *   - loop through each card and create its HTML
 *   - add each card's HTML to the page
 */
function shuffleDeck()  {
  const cardsToShuffle = Array.from(document.querySelectorAll('.deck li'));
  console.log('Cards to shuffle', cardsToShuffle);
  const shuffledCards = shuffle(cardsToShuffle);
  for(card of shuffledCards)  {
    deck.appendChild(card);
  }
}
shuffleDeck();





// Shuffle function from http://stackoverflow.com/a/2450976
function shuffle(array) {
    var currentIndex = array.length, temporaryValue, randomIndex;

    while (currentIndex !== 0) {
        randomIndex = Math.floor(Math.random() * currentIndex);
        currentIndex -= 1;
        temporaryValue = array[currentIndex];
        array[currentIndex] = array[randomIndex];
        array[randomIndex] = temporaryValue;
    }

    return array;
}


/*
 * event listener
 */
const cards = document.querySelectorAll('.card');
console.log(cards);

deck.addEventListener('click', event =>  {
  const clickTarget = event.target;
  if (clickTarget.classList.contains('card') &&
  !clickTarget.classList.contains('match') &&
  toggledCards.length < 2  &&
  !toggledCards.includes(clickTarget)
) {
    if (clockOff)  {
        startClock();
        clockOff = false;
    }
     toggleCard(clickTarget);
     addToggleCard(clickTarget);
     if (toggledCards.length === 2)  {
         checkForMatch(clickTarget);
         addMove();
         checkScore();
     }
  }
});

function toggleCard(clickTarget) {
  clickTarget.classList.toggle('open');
  clickTarget.classList.toggle('show');
}

function addToggleCard(clickTarget)  {
  toggledCards.push(clickTarget);
  console.log(toggledCards);
}

function checkForMatch()  {
    if (
      toggledCards[0].firstElementChild.className ===
      toggledCards[1].firstElementChild.className
    ) {
      toggledCards[0].classList.toggle('match');
      toggledCards[1].classList.toggle('match');
      console.log('Match!');
      matched++;
      checkWin();
      toggledCards = [];
    } else {
      setTimeout(()  =>  {
      //console.log('Not a match!');
      toggleCard(toggledCards[0]);
      toggleCard(toggledCards[1]);
      toggledCards = [];
    }, 1000);
}
}