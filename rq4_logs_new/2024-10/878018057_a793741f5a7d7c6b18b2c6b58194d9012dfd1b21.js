const user_click = document.querySelectorAll(".user_click");

const userScore = document.querySelector("#user_score");
const compScore = document.querySelector("#comp_score");

const msg = document.querySelector("#result");

let userScores  = 0;
let compScores = 0;

const click = (user) =>{
    let choices = ["rock","paper","scissors"];
    let comp = choices[Math.floor(Math.random()*3)];
    if (comp === user){
        msg.innerText = "It's a draw!";
        msg.style.backgroundColor = " rgb(4, 4, 58)";
    }
    else{
        let win = true;
        if (user === "rock") {
            win = comp === "paper" ? false : true;
        }
        else if (user === "paper") {
            win = comp === "scissors" ? false : true;
        }
        else if (user === "scissors") {
            win = comp === "rock" ? false : true;
        }
        if(win){
            msg.innerText = `You win! The computer chose ${comp}.`;
            msg.style.backgroundColor = "green";
            userScores++;
            userScore.innerText = userScores;
        }
        else{
            msg.innerText = `You lose! The computer chose ${comp}.`;
            msg.style.backgroundColor = "red";
            compScores++;
            compScore.innerText = compScores;
        }
    }
};

user_click.forEach(box => {
    box.addEventListener("click",() =>{
        let user = box.getAttribute("id");
        click(user);
    });
});