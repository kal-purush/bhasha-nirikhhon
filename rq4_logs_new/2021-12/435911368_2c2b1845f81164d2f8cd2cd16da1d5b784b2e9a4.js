function generateNum(minimum, maximum)
{
    return Math.floor(Math.random() * (maximum - minimum + 1)) + minimum;
}

function computerPlay()
{
    const val = generateNum(0,2);
    const table = {
        0: "Rock",
        1: "Paper",
        2: "Scissors"
    };
    return table[val];
}


/* PLAYGROUND LOGIC
===================
    IF WHAT THE COMPUTER SELECTED IS THE COMPLEMENT OF {PLAYER} IN 'WINTABLE', I WIN
    ELSE
        I LOSE
*/

function playRound(playerSelection, computerSelection)
{
    playerSelection = playerSelection.toUpperCase();
    computerSelection = computerSelection.toUpperCase();

    let winTable = {
        "ROCK" : "SCISSORS",
        "SCISSORS" : "PAPER",
        "PAPER" : "ROCK"
    };

    if (playerSelection === computerSelection)
    {
        return -1;
    }
    else if (computerSelection == winTable[playerSelection])
    {
        return 1;
    }
    else
    {
        return 0;
    }
}

/* LOGIC OF GAME
=================
    LOOP THROUGH 5 TIMES, AT EACH ITERATION
        PLAY A ROUND AND RECORD SCORE OF WINNER AND LOSER
 */

function game()
{
    let user = 0;
    let comp = 0;

    for (let i = 0; i < 5; i++)
    {
        let compPick = computerPlay();
        let userPick = prompt("Enter your pick [Rock, Paper, Scissors]");
        let result = playRound(userPick, compPick);
        if (result === 1) ++user;
        else if (result === 0) ++comp;
    }

    if (user > comp) return "User Wins!"
    else if (comp > user) return "Computer Wins!"
    else return "Tie!";
}

console.log(game());