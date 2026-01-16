function computerPlay() {
    const comp = ["rock", "paper", "scissors"];
    let attack = Math.floor(Math.random() * 3);

    return comp[attack];
}



function playRound(playerSelection, computerSelection) {
    let player = playerSelection.toLowerCase();
    let comp = computerSelection.toLowerCase();
    if(player === comp){
        return "DRAW";
    }
    else if (player === "rock" && comp === "scissors"
            || player === "paper" && comp === "rock"
            || player === "scissors" && comp === "paper"){
                
                return "PLAYER";

    }
    else {
        return "COMP";
    }
}




function game() {
    const player = prompt("input rock,paper, or scissors", "rock");
    const computer = computerPlay();

    console.log(playRound(player,computer))
}

game();