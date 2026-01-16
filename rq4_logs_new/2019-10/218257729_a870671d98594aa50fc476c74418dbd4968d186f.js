const game = () => {
  let pScore = 0;
  let cScore = 0;

  //Start the Game
  const startGame = () => {
    const playBtn = document.querySelector(".intro button");
    const introScreen = document.querySelector(".intro");
    const match = document.querySelector(".match");

    playBtn.addEventListener("click", () => {
      introScreen.classList.add("fadeOut");
      match.classList.add("fadeIn");
  
    });
  };
  //Play Match
  const playMatch = () => {
    const options = document.querySelectorAll(".options button");
    const playerHand = document.querySelector(".player-hand");
    const computerHand = document.querySelector(".computer-hand");
    const hands = document.querySelectorAll(".hands img");

    hands.forEach(hand => {
      hand.addEventListener("animationend", function() {
        this.style.animation = "";
      });
    });
    //Computer Options
    const computerOptions = ["rock", "paper", "scissors"];

    options.forEach(option => {
      option.addEventListener("click", function() {
        //Computer Choice
        const computerNumber = Math.floor(Math.random() * 3);
        const computerChoice = computerOptions[computerNumber];

        setTimeout(() => {
          //Here is where we call compare hands
          compareHands(this.className, computerChoice);
          //When someone got 4 points
          // if (pScore==4 || cScore==4) {
          //   document.querySelector(".visibility_matchpoint").style.visibility="visible";
          //   console.log("hi")
          // }
          // if(pScore==5 || cScore==5){
          //   console.log("hello")
          //   document.querySelector(".visibility_matchpoint").style.visibility="hidden";
          //   document.querySelector(".finish").innerText= pScore==5 ? "あなたの勝ちです！" : "あなたの負けです..";
          //   pScore = 0;
          //   cScore = 0;
          // }

          //Update Images
          playerHand.src = `./assets/${this.className}.png`;
          computerHand.src = `./assets/${computerChoice}.png`;
        }, 2000);
        //Animation
        playerHand.style.animation = "shakePlayer 2s ease";
        computerHand.style.animation = "shakeComputer 2s ease";
      });
    });
  };

  const updateScore = () => {
    const playerScore = document.querySelector(".player-score p");
    const computerScore = document.querySelector(".computer-score p");
    playerScore.textContent = pScore;
    computerScore.textContent = cScore;
  };
  const moveOnBoard = (score, u=true) =>{
    let field_lis = document.getElementsByClassName('field')[0].getElementsByTagName('li');
    if(u){
      field_lis[pScore].innerText="";
      if(pScore == cScore){
        field_lis[pScore].innerText="太朗";
      }
      pScore+= score;
      field_lis[pScore].innerText= pScore == cScore ? "あなた<br>太朗" : "あなた";
    }else{
      field_lis[cScore].innerText="";
      if(pScore == cScore){
        field_lis[pScore].innerText="あなた";
      }
      cScore+= score;
      field_lis[cScore].innerText= pScore == cScore ? "あなた<br>太朗" : "太朗";
    }
    
  }
  const compareHands = (playerChoice, computerChoice) => {
    //Update Text
    const winner = document.querySelector(".winner");
    //Checking for a tie
    if (playerChoice === computerChoice) {
      winner.textContent = "引き分けか・・・";
      return;
    }
    //Check for Rock
    if (playerChoice === "rock") {
      if (computerChoice === "scissors") {
        winner.textContent = "あなたの勝ち！";
        // pScore+=2;
        moveOnBoard(2);
        updateScore();
        return;
      } else {
        winner.textContent = "じゃんけん太郎の勝ち！";
        // cScore+=4;
        moveOnBoard(4, false);
        updateScore();
        return;
      }
    }
    //Check for Paper
    if (playerChoice === "paper") {
      if (computerChoice === "scissors") {
        winner.textContent = "じゃんけん太郎の勝ち！";
        // cScore+=5;
        moveOnBoard(5, false);
        updateScore();
        return;
      } else {
        winner.textContent = "あなたの勝ち！";
        // pScore+=4;
        moveOnBoard(4);
        updateScore();
        return;
      }
    }
    //Check for Scissors
    if (playerChoice === "scissors") {
      if (computerChoice === "rock") {
        winner.textContent = "じゃんけん太郎の勝ち！";
        // cScore+=2;
        moveOnBoard(2, false);
        updateScore();
        return;
      } else {
        winner.textContent = "あなたの勝ち！";
        // pScore+=5;
        moveOnBoard(5);
        updateScore();
        return;
      }
    }
  };

  //Is call all the inner function
  startGame();
  playMatch();
};

//start the game function
game();