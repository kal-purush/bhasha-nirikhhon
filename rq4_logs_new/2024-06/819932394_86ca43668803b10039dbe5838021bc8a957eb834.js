// * ---- Pseudocode ----


// - As a user, when the page loads I want to be able to start the game
// * Click start button
//   > Add click event to start button, it should execute a function called startGame()
// * Remove button from screen
//   > Target the hangman <section>
//   > Remove the '.game-over' class
//   > Add the '.game-active' class
// * Display the spaces for the word to be guessed
//   > Generate a word to be guessed. Define an array of words. Use Math floor & random to get a random word from the array.
//   > Save random word to wordToGuess
//   > To display the correct amount of spaces we'll use wordToGuess.length
//   > Using a for loop, set it to loop wordToGuess.length amount of times
//   > In each loop: create a div, add class .guessed-letter, append to guessContainer
// * Game state displayed on screen, e.g how many attempts have been made (maybe how many I have left)
//   > Game state section (.hangman) will be displayed when .game-active is added above
//   > using the attemptsMade, update the attemptsSpan with it's value
//   > attemptsSpan.innerText = attemptsMade

// - As a user, when the game starts I want to be able to clearly see the length of the word I'm guessing

// - As a user, I want to see that my guessed letter has been registered
// * Click event on the keys within the keyboard
//   > Target the keys with `.key` class. Add a click event, executing submitLetter()
// * Attempts number should increase by 1
//   > attemptsMade should increase by 1
//   > attemptsSpan.innerText = attemptsMade
// * If a letter has been guessed correctly, we should see the empty spaces fill with the correct letter
//   > identify which letter has been clicked
//   > check every letter of wordToGuess, if it matches the clicked letter, display it

// - As a user, I want to see if any of the letters that I choose are in the word I'm guessing in their correct position

// - As a user, I want to see how many attempts I've made, and how many I have left

// - As a user, I want to see clearly when I have won the game
// * attemptsMade === allowedAttempts, endGame()
// * Update the messageDisplay when the game is won
//   > messageDisplay.innerText = 'You won!'

// - As a user, I want to see clearly when I have lost the game
// * Update the messageDisplay when the game is won
//   > messageDisplay.innerText = 'You lost!'

// - As a user, I want to be able to play again after I have finished a game
// * Add start button back to screen
//   > Target the hangman <section>
//   > Remove the '.game-active' class
//   > Add the '.game-over' class
//   > Target the button, update its innerText to 'Restart Game'






/*-------------- Constants -------------*/
const allowedGuesses = 8
const snowManWords = ['boy', 'girl', 'food', 'monkey']
const letterEls = []

/*---------- Variables (state) ---------*/

let attemptsMade = 1 //  - this will hold the number of attempts made so far
let correctGuesses = 0 
let wordToGuess = [];
let currentWord

// * ---- DOM Elements ----
const startGameButton = document.querySelector('.start-game')
const attemptsWrapper = document.querySelector('.attempts-wrapper')
const snowmanImage = document.querySelector('#snowman-img')
const guessContainer = document.querySelector('.guess-container')
const keyElements = document.querySelectorAll('.key')
const snowmanSection = document.querySelector('.snowman')
const messageDisplay = document.querySelector('.message')
const playAgainButton = document.querySelector('.play-again')
const snowmanGameOver = document.querySelector('.snowman-gameover')



// * ---- Functions ----

const hide = () => {
    startGame.display = 'none'
    playAgain.display = 'none'
}


const startGame = () => {
    startGameButton.innerText = 'Play Again'
    keyElements.forEach(key => key.disabled = false)
    startGameButton.classList.add('hide')
    attemptsMade = 1
    correctGuesses = 0
    messageDisplay.innerText = ''
    guessContainer.innerHTML = ''
    wordToGuess = snowManWords[Math.floor(Math.random() * snowManWords.length)];
    for (let i = 0; i < wordToGuess.length; i++) {

    let newDiv = document.createElement('div')
    newDiv.classList.add('letter-space')
    letterEls.push(newDiv)
    guessContainer.appendChild(newDiv) //append child

    attemptsWrapper.innerText = attemptsMade

    updateUI()

        

    console.log(wordToGuess);
    }
    console.log('startGame')

    console.log('key')

}



//keyElements = document.querySelectorAll('.keys').forEach(key => key.disabled = false);

//attemptsMade = 0 
//removeButton
//generateElement
//updateUI

// * Click start button
//   > Add click event to start button, it should execute a function called startGame()
// * Remove button from screen
//   > Target the hangman <section>
//   > Remove the '.game-over' class
//   > Add the '.game-active' class

//* Display the spaces for the word to be guessed
//   > Generate a word to be guessed. Define an array of words. Use Math floor & random to get a random word from the array.
//   > Save random word to wordToGuess
//   > To display the correct amount of spaces we'll use wordToGuess.length
//   > Using a for loop, set it to loop wordToGuess.length amount of times
//   > In each loop: create a div, add class .guessed-letter, append to guessContainer
// * Game state displayed on screen, e.g how many attempts have been made (maybe how many I have left)
//   > Game state section (.hangman) will be displayed when .game-active is added above
//   > using the attemptsMade, update the attemptsSpan with it's value
//   > attemptsSpan.innerText = attemptsMade


const key = () => {

    console.log('key')

}

const updateUI = () => {
    attemptsWrapper.innerText = `❤️ ${allowedGuesses-attemptsMade}`
    snowmanImage.src = `Images/Snowman-${attemptsMade}.jpg`
}


const increaseAttempts = () => {
    attemptsMade += 1

    updateUI()

}


const submitLetter = (evt) => {

    const key = evt.target.innerText
    evt.target.disabled = true
    if (wordToGuess.includes(key)) {
        for (let i in wordToGuess) {
            if (wordToGuess[i] === key) {
                letterEls[i].innerText = key.toUpperCase()
                correctGuess()
            }
        }

    } else {
        increaseAttempts()
    }

    if (attemptsMade === allowedGuesses) {
        endGame('loser!')
    } 
    
    if (wordToGuess.length === correctGuesses){ 
        endGame('You won!')    
    }



    // * Attempts number should increase by 1
    //   > attemptsMade should increase by 1
    // attemptsWrapper.innerText = attemptsMade

    // * If a letter has been guessed correctly, we should see the empty spaces fill with the correct letter
    //   > identify which letter has been clicked
    //   > check every letter of wordToGuess, if it matches the clicked letter, display it

    // * attemptsMade === allowedAttempts, endGame()
    // * Update the messageDisplay when the game is won
    //   > messageDisplay.innerText = 'You won!'

    // * Update the messageDisplay when the game is won
    //   > messageDisplay.innerText = 'You lost!'
}

const endGame = (message) => {
    messageDisplay.innerText = message
    keyElements.forEach(key => key.disabled = true);
    startGameButton.classList.remove('hide')
}

const playAgain = () => {
    playAgainButton.classList.add('hide')
    keyElements.forEach(key => key.disabled = false)
}

const correctGuess = () => {
    correctGuesses ++ 

    updateUI()

}


// * ---- On Page Load ----


// * ---- Events ----

startGameButton.addEventListener('click', startGame)
keyElements.forEach(el => el.addEventListener('click', key))
keyElements.forEach(el => el.addEventListener('click', submitLetter)
)