function getRandomInt(max)
{
    return Math.floor(Math.random() * max);
}

function getComputerChoice()
{
    let num = getRandomInt(3);
    let choice;

    if(num === 0)
    {
        choice = 'rock';
    }
    else if(num === 1)
    {
        choice = 'paper';
    }
    else if(num === 2)
    {
        choice = 'scissor';
    }

    return choice;
}

function getHumanChoice()
{
    let choice = prompt("Enter rock, paper or scissor");
    return choice;
}

let humanScore = 0;
let computerScore = 0;

function playRound(humanChoice, computerChoice)
{
    humanChoice = humanChoice.toLowerCase();
    
    if(humanChoice === computerChoice)
    {
        console.log("It's a tie");
    }
    else if((humanChoice === "rock") && (computerChoice === "scissor"))
    {
        console.log("You win! rock beats scissor");
        humanScore++;
    }
    else if((humanChoice === "paper") && (computerChoice === "rock"))
    {
        console.log("You win! paper beats rock");
        humanScore++;
    }
    else if((humanChoice === "scissor") && (computerChoice === "paper"))
    {
        console.log("You win! scissor beats paper");
        humanScore++;
    }
    else if((humanChoice === "rock") && (computerChoice === "paper"))
    {
        console.log("You lose! paper beats rock");
        computerScore++;
    }
    else if((humanChoice === "paper") && (computerChoice === "scissor"))
    {
        console.log("You lose! scissor beats paper");
        computerScore++;
    }
    else if((humanChoice === "scissor") && (computerChoice === "rock"))
    {
        console.log("You lose! rock beats scissor");
        computerScore++;
    }
}
  
function playGame()
{
    for(let i = 0; i < 5; i++)
    {
        let humanSelection = getHumanChoice();
        let computerSelection = getComputerChoice();
  
        playRound(humanSelection, computerSelection);
    }

    if (humanScore > computerScore)
    {
        console.log("You Win");
    }
    else
    {
        console.log("You Lose");
    }

    console.log('Your points : %s', humanScore);
    console.log('Computer points : %s', computerScore);
}

playGame();