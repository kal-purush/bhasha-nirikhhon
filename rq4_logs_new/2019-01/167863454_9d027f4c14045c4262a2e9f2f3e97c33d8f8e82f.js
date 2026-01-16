//bowie help
// function clickTracker(event) {
//     const key = event.key.toLowerCase();

//     if (key === randomizer) {
//         // increase win count
//         win = win + 1;
//         winTracker.innerHTML = win;
//         // reset game
//         resetGame();
//     }
//     else {



// Collapse 
//guessing and letter staying
//double for loop. looping thru


































































































 // Global Variables
 var wordBank = ['corgi', 'pitbull', 'husky'];
 var wins = 0;
 var loss = 0;
 var wrongLetter = [];
 var guessesLeft = 9;
 var underscores = [];
 var userGuesses = []
 var randWord; 


// Function

function startGame() {
    //picks random word
    console.log(wordBank [2]);
    randWord = wordBank[Math.floor(Math.random() * wordBank.length)].split("");
    console.log ('random Word = ' + randWord)
    for(var i = 0; i < randWord.length; i++)
    {
            underscores.push('_');
    }
    // printing underscores to the screen
    document.getElementById('word-blanks').textContent = underscores.join(" ");
    console.log(underscores)
    //reset
    wrongLetter = [];
    guessesLeft = 9;

        // html
    document.getElementById('guesses-left').textContent = guessesLeft;

}
function winLose()
{
  
}
// User Guessses
document.onkeyup = function(event)
{
    userGuesses = event.key;
    console.log(randWord);

    // checking if the letter exist inside the word
    if (randWord.indexOf(userGuesses) > -1)
    {
        for(var i = 0; i < randWord.length; i++)
        {
            if(randWord[i] === userGuesses)
            {
            underscores[i] = userGuesses;
            console.log(underscores);    
            }
            else {
                console.log(" ");
            }
        }
    }
    else 
    {
    wrongLetter.push(userGuesses);
    guessesLeft--;
    console.log(guessesLeft);
    console.log(wrongLetter);
    }
}
 

// Main
startGame();
