
//alert("Welcome to Rock-Paper-Scissors.");
var pScore = 0;
var cScore = 0;
let playerScore = document.querySelector('#player-score');
let playerText = playerScore.innerText;
let compScore = document.querySelector('#computer-score');
let compText = compScore.innerText;
let roundNumber = document.querySelector('#round-number');
let roundText = roundNumber.innerText;

let choices = document.querySelectorAll('.choice');
choices.forEach((choice)=> choice.addEventListener('click',startRound));
function startRound(){
    playRound(this.id, computerPlay());
}

function playRound(playerSelection,computerSelection){

    switch(true){
        case playerSelection === "rock" && computerSelection == "scissors":
        case playerSelection === "paper" && computerSelection == "rock":
        case playerSelection === "scissors" && computerSelection == "paper":
            console.log(`You Win! ${playerSelection} beats ${computerSelection}`);
            pScore++;
            break;
        case playerSelection == computerSelection:
            console.log(`It was a tie! You both chose ${playerSelection}`);
            pScore+=.5;
            cScore+=.5
            break;
        default:
            console.log(`You Lose! ${computerSelection} beats ${playerSelection}`);
            cScore++;     
    }

    playerText = playerText.substr(0,8) + pScore;
    playerScore.innerText = playerText;
    compText = compText.substr(0,10) + cScore;
    compScore.innerText = compText;
    roundText = roundText.substr(0,15) + (cScore+pScore+1);
    roundNumber.innerText = roundText;

    if(pScore >= 5 || cScore >= 5){
        endGame();
    }
}

function endGame(){
    if(pScore > cScore){
        console.log("You won the game!");
    } else if (cScore > pScore){
        console.log("The computer won the game!");
    } else {
        console.log("It was a tie!");
    }
    console.log(`Final Scores--> Player:${pScore} | Computer:${cScore}`);
    console.log("Thanks for playing.");
    
    pScore = 0;
    cScore = 0;

    let response = prompt('Enter "yes" to play again');
    if(response.toLowerCase() === "yes"){
        reset();
    } else {
        choices.forEach((choice)=> choice.removeEventListener('click',startRound));
    }
}

function reset(){
    playerText = playerText.substr(0,8) + 0;
    playerScore.innerText = playerText;
    compText = compText.substr(0,10) + 0;
    compScore.innerText = compText;
    roundText = roundText.substr(0,15) + 1;
    roundNumber.innerText = roundText;
}

function computerPlay(){
    let value = Math.floor(Math.random()*3);
    let result = (value == 0)? "rock":
    (value==1)? "paper": "scissors";
    return result;
}