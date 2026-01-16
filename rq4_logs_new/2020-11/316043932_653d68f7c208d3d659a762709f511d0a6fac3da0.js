var p1Button = document.querySelector("#p1");
var p2Button = document.getElementById("p2");
var p1Display = document.querySelector("#p1Display");
var p2Display = document.getElementById("p2Display");
var p1Score = 0;
var p2Score = 0;
var gameOver = false;
var winningScore = 5;
var winningScoreDisplay = document.querySelector("p span");
var numInput = document.querySelector("input");
var resetButton = document.querySelector("#reset");
var winner = document.getElementById("winner");

p1Button.addEventListener("click", function(){
    if(!gameOver){
        p1Score++;
        if(p1Score === winningScore){
            p1Display.classList.add("winner");
            gameOver = true;
            winner.textContent = "Player One wins!";
        }
        p1Display.textContent = p1Score;
    }
});

p2Button.addEventListener("click", function(){
    if(!gameOver){
        p2Score++;
        if(p2Score === winningScore){
            p2Display.classList.add("winner");
            gameOver = true;
            winner.textContent = "Player Two wins!";
        }
        p2Display.textContent = p2Score;
    }
});

resetButton.addEventListener("click", function(){
   reset();
});

numInput.addEventListener("change", function(){
    winningScoreDisplay.textContent = numInput.value;
    winningScore = Number(numInput.value);
    reset();
});

function reset(){
    p1Score = 0;
    p2Score = 0;
    p1Display.textContent = 0;
    p2Display.textContent = 0;
    gameOver = false;
    winner.textContent = "";
    p1Display.classList.remove("winner");
    p2Display.classList.remove("winner");
}