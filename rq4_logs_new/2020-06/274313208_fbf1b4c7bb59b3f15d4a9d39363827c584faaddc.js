// Create references to Start Match, Rock, Paper, and Scissors buttons
// Create references for player and computer card images, text, result, score, history
var startButton = d3.select("#start");
var rockButton = d3.select("#rock");
var paperButton = d3.select("#paper");
var scissorsButton = d3.select("#scissors");
var playerImage = d3.select("#choice-player").select("img");
var computerImage = d3.select("#choice-computer").select("img");
var playerText = d3.select("#choice-player").select("p");
var computerText = d3.select("#choice-computer").select("p");
var resultText = d3.select("#winner");
var playerTally = 0;
var computerTally = 0;
var playerScore = d3.select("#score").select("#player");
var computerScore = d3.select("#score").select("#computer");
var roundHistory = d3.select("#history-round").select("li");
var playerHistory = d3.select("#history-player").select("li");
var computerHistory = d3.select("#history-computer").select("li");



// Create a click event on SM button
startButton.on("click", function() {
    // Disbable SM button; enable RPS buttons
    startButton.attr("disabled", "disabled");
    rockButton.attr("disabled", null);
    paperButton.attr("disabled", null);
    scissorsButton.attr("disabled", null);
});

// Create variables used during the match
var playerSelection;
var computerSelection;
var winner;
var playerSelections = [];
var computerSelections = [];
var winners = [];

// Create function to determine computer's selection
function determineComputerSelection() {
    var randomValue = Math.random();
    switch(true) {
        case (randomValue < 1/3):
            computerSelection = "Rock";
            break;
        case (randomValue < 2/3):
            computerSelection = "Paper";
            break;
        default:
            computerSelection = "Scissors";
    };
    computerSelections.push(computerSelection);
};

// Create function to determine winner of round
function determineWinner(playerSelection, computerSelection) {
    if (playerSelection === "Rock") {
        if (computerSelection === "Rock") {
            winner = "Tie";
        } else if (computerSelection === "Paper") {
            winner = "Computer";
        } else {
            winner = "Player";
        }
    } else if (playerSelection === "Paper") {
        if (computerSelection === "Rock") {
            winner = "Player";
        } else if (computerSelection === "Paper") {
            winner = "Tie";
        } else {
            winner = "Computer";
        }
    } else {
        if (computerSelection === "Rock") {
            winner = "Computer";
        } else if (computerSelection === "Scissors") {
            winner = "Tie";
        } else {
            winner = "Player";
        }
    };
    winners.push(winner);
};

// Create function to update HTML with selections and winner
function updateResults(playerSelection, computerSelection, winner) {
    playerImage.attr("src", `images/${playerSelection.toLowerCase()}.png`);
    computerImage.attr("src", `images/${computerSelection.toLowerCase()}.png`);
    playerText.html(`Player chooses ${playerSelection}`);
    computerText.html(`Computer chooses ${computerSelection}`);
    switch(winner) {
        case "Player":
            resultText.html(`${winner} Wins!`);
            resultText.attr("style", "color:blue");
            playerTally += 1;
            playerScore.html(`<strong>Player: </strong>${playerTally}`);
            break;
        case "Computer":
            resultText.html(`${winner} Wins!`);
            resultText.attr("style", "color:red");
            computerTally += 1;
            computerScore.html(`<strong>Computer: </strong>${computerTally}`);
            break;
        default:
            resultText.html("It's a Draw!");
            resultText.attr("style", "color:black");
    };
    roundHistory.html(playerSelections.length)
    playerHistory.html(playerSelection.charAt(0));
    computerHistory.html(computerSelection.charAt(0));
};

// Create click events on RPS buttons
rockButton.on("click", function() {
    playerSelection = "Rock";
    playerSelections.push(playerSelection);
    determineComputerSelection();
    determineWinner(playerSelection, computerSelection);
    updateResults(playerSelection, computerSelection, winner);
    // console.log(`Player: ${playerSelection}`);
    console.log(playerSelections);
    // console.log(`Computer: ${computerSelection}`);
    console.log(computerSelections);
    // console.log(`Winner: ${winner}`);
    // console.log(winners);
});

paperButton.on("click", function() {
    playerSelection = "Paper";
    playerSelections.push(playerSelection);
    determineComputerSelection();
    determineWinner(playerSelection, computerSelection);
    updateResults(playerSelection, computerSelection, winner);
    console.log(playerSelections);
    console.log(computerSelections);
});

scissorsButton.on("click", function() {
    playerSelection = "Scissors";
    playerSelections.push(playerSelection);
    determineComputerSelection();
    determineWinner(playerSelection, computerSelection);
    updateResults(playerSelection, computerSelection, winner);
    console.log(playerSelections);
    console.log(computerSelections);
});