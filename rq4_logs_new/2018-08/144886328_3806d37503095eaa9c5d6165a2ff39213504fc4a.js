let playerScore = 0;
let computerScore = 0;

function computerPlay(){
  let randomIndex = Math.floor(Math.random() * 5);
  let choice = ["Rock", "Paper", "Scissors", "Lizard", "Spock"];
  return choice[randomIndex];
}

function playRound(playerSelection, computerSelection) {
  if(playerSelection===computerSelection){
    return "Tie!";
  }else if (playerSelection=="Rock"){
    if(computerSelection=="Paper" || computerSelection=="Spock"){
      return "Loss!";
    }
    if(computerSelection=="Scissors" || computerSelection=="Lizard"){
      return "Point!"
    }
  }else if (playerSelection=="Paper"){
    if(computerSelection=="Scissors" || computerSelection=="Lizard"){
      return "Loss!";
    }
    if(computerSelection=="Rock" || computerSelection=="Spock"){
      return "Point!"
    }
  }else if (playerSelection=="Scissors"){
    if(computerSelection=="Rock" || computerSelection=="Spock"){
      return "Loss!";
    }
    if(computerSelection=="Paper" || computerSelection=="Lizard"){
      return "Point!"
    }
  }else if (playerSelection=="Lizard"){
    if(computerSelection=="Scissors" || computerSelection=="Rock"){
      return "Loss!";
    }
    if(computerSelection=="Paper" || computerSelection=="Spock"){
      return "Point!"
    }
  }else if (playerSelection=="Spock"){
    if(computerSelection=="Paper" || computerSelection=="Lizard"){
      return "Loss!";
    }
    if(computerSelection=="Scissors" || computerSelection=="Rock"){
      return "Point!"
    }
  }
}

function game(clicked_id){
  let message = "";
  if(playerScore < 5 && computerScore < 5){
    let playerSelection = clicked_id;
    let computerSelection = computerPlay();
    let value = playRound(playerSelection, computerSelection);
    message += value + " ";
    if(value === "Loss!"){
      computerScore++;
      message += ("Computer selected: " + computerSelection + ". You selected: " + playerSelection + "!");
    }else if (value === "Point!") {
      playerScore++;
      message += ("Computer selected: " + computerSelection + ". You selected: " + playerSelection + "!");
    }else if (value === "Tie!"){
      message += "Both selected " + playerSelection;
    } 
    console.log("Player points: " + playerScore + " Computer Points: " + computerScore);
  }

  if(playerScore == 5){
      message = "You win! Congratulations!";
    }else if(computerScore == 5){
      message = "You lose! Sorry :(";
    }
    document.getElementById("compScore").innerHTML = computerScore;
    document.getElementById("userScore").innerHTML = playerScore;
    document.getElementById("result").innerHTML = message;
}

function reset(){
  playerScore = 0;
  computerScore = 0;
  document.getElementById("compScore").innerHTML = computerScore;
  document.getElementById("userScore").innerHTML = playerScore;
  document.getElementById("result").innerHTML = "";
}