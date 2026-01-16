"use strict";

let computerScore = 0;
let playerScore = 0;
let styleColor;
        
function computerPlay() {
const rand = Math.floor(Math.random() * 3);
            
    switch (rand) {
        case 0:
            return "Rock";
            break;
        case 1:
            return "Paper";
            break;
        case 2:
            return "Scissors";
            break;
    }
            
}

function playRound(playerSelection, computerSelection) {
    playerSelection = playerSelection.toLowerCase();
            
    const upperCaseFirstLetter = playerSelection.charAt(0).toUpperCase();
    playerSelection = upperCaseFirstLetter + playerSelection.slice(1);  

    switch (true) {
        case playerSelection === "Rock" && computerSelection === "Scissors":
        case playerSelection === "Paper" && computerSelection === "Rock":
        case playerSelection === "Scissors" && computerSelection === "Paper":
            playerScore++;
            styleColor = "green";
            return `You Win! ${playerSelection} ${setSingularPlural(playerSelection)} ${computerSelection}!`;
            break;
        case computerSelection === "Rock" && playerSelection === "Scissors":
        case computerSelection === "Paper" && playerSelection === "Rock":
        case computerSelection === "Scissors" && playerSelection === "Paper":
            computerScore++;
            styleColor = "red";
            return `You Lose! ${computerSelection} ${setSingularPlural(playerSelection)} ${playerSelection}!`;
            break;
        default:
            styleColor = "blue";
            return `${playerSelection} vs ${computerSelection}! It's a draw!`;
    }          
}

function setSingularPlural(playerSelection) {
    if (playerSelection === "Scissors") {
        return "beat";
    } else {
        return "beats";
    }
}

function game() {
    let playerSelection;
    let gameRounds = 0;
    const resultStyle = "color: white; font-size: 20px;";
            
    while (gameRounds < 5) {
        console.log(`%cRound ${gameRounds + 1}:`, "font-weight: bold");
        playerSelection = prompt("Which weapon do you pick? Rock, Paper, or Scissors?", '');
                
        if (!playerSelection) {
            console.clear();
            return;
        } 

        switch (playerSelection.toLowerCase()) {
            case "rock":
            case "paper":
            case "scissors":
                console.log(`%c${playRound(playerSelection, computerPlay())}`, `color: ${styleColor}`);
                console.log(`Player: ${playerScore}, Computer: ${computerScore}`);
                gameRounds++;
                break;
            default:
                alert("You have to submit a valid value!");
        }
    }
            
    if (playerScore > computerScore) {
        console.log("%cYOU WON! YOU'RE A WINNER!", `${resultStyle} background-color: green;`);
    } else if (playerScore < computerScore) {
        console.log("%cYOU LOST! YOU ARE A LOSER!", `${resultStyle} background-color: red;`);
    } else {
        console.log("%cIt's a draw...", `${resultStyle} background-color: blue;`);
    }
}

//game();