//disabling Right-click
document.addEventListener('contextmenu', event => event.preventDefault());

let user_score = 0;
let cpu_score = 0;

/*caching the dom variables to use it later by calling only variable names
(We don't have to use Document.getElementById repeatedly)*/

const user_scoreSpan = document.getElementById("userScore");
const cpu_scoreSpan = document.getElementById("cpuScore");
const score_boardDiv = document.querySelector(".scoreBoard");
const resultDiv_p = document.querySelector(".result > p");
const rockDiv = document.getElementById("r");
const paperDiv = document.getElementById("p");
const scissorDiv = document.getElementById("s");
let ResetButton = document.querySelector(".resetBtn");

/*Setting up the Cpu's random choices by using Math Function */

function getCpuChoice(){
    const choices = ['r','p','s'];
    const randomNumber = Math.floor(Math.random() * 3);
    return choices[randomNumber];
}

// converting letter to words for displaying properly inside the result

function convertToWord(move) {
    if (move === "r") return "Rock";
    if (move === "p") return "Paper";
    return "Scissors";
}

// adding a condition for scoreboard and result

function win(userChoice, cpuChoice){
    user_score++;
    user_scoreSpan.innerHTML = user_score;
    cpu_scoreSpan.innerHTML = cpu_score;
    resultDiv_p.innerHTML = `${convertToWord(userChoice)} beats ${convertToWord(cpuChoice)}. <span style="color:green;">You Win!</span>`;
    document.getElementById(userChoice).classList.add('greenGlow');
    setTimeout(function() { document.getElementById(userChoice).classList.remove('greenGlow') }, 1000);
}

function lose(userChoice, cpuChoice) {
    cpu_score++;
    user_scoreSpan.innerHTML = user_score;
    cpu_scoreSpan.innerHTML = cpu_score;
    resultDiv_p.innerHTML = `${convertToWord(cpuChoice)} beats ${convertToWord(userChoice)}. <span style="color:red;">You Lose!</span>`;
    document.getElementById(userChoice).classList.add('redGlow');
    setTimeout(function() { document.getElementById(userChoice).classList.remove('redGlow') }, 1000);
}

function draw(userChoice, cpuChoice) {
    resultDiv_p.innerHTML = `${convertToWord(userChoice)} is equals to ${convertToWord(cpuChoice)}. <span style="color:grey;">It's a Draw</span>`;
    document.getElementById(userChoice).classList.add('greyGlow');
    setTimeout(function() { document.getElementById(userChoice).classList.remove('greyGlow') }, 1000);
}

/* Rock < Paper < Scissor < (loop) */

function game(userChoice) {
    const cpuChoice = getCpuChoice();
    switch (userChoice + cpuChoice) {
        case "rs":       /* User Win Case */
        case "pr":
        case "sp":
            win(userChoice, cpuChoice);
            break;

        case "rp":      /* User Lose Case */
        case "ps":
        case "sr":
            lose(userChoice, cpuChoice);
            break;

        case "rr":      /* Draw Case */
        case "pp":
        case "ss":
            draw(userChoice, cpuChoice);
            break;
    }
}

function main() {

    rockDiv.addEventListener('click', function(){
        game("r");
    })

    paperDiv.addEventListener('click', function(){
        game("p");
    })

    scissorDiv.addEventListener('click', function(){
        game("s");
    })
}

main();

// Reset Button

function resetGame() {
    user_score = 0;
    cpu_score = 0;
    user_scoreSpan.innerHTML = user_score;
    cpu_scoreSpan.innerHTML = cpu_score;
    resultDiv_p.innerHTML = "Make Your Move.";
}

ResetButton.addEventListener('click', resetGame);

// Rules Button

document.querySelector(".rulesBtn").addEventListener('click', function(){
    document.getElementById("infoBox").style.display = "block";
});

document.querySelector(".close-btn").addEventListener('click', function(){
    document.getElementById("infoBox").style.display = "none";
});