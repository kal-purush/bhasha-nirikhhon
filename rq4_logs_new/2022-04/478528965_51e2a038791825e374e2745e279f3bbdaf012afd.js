
// I have asisgn all variables before hand becouse i want use then also in other functions 


let messageEl = document.getElementById("message-el");
let sumEl =  document.getElementById("sum-el");
let cardEl = document.getElementById("cards-el");
let firstCard = 0;
let secondCard = 0;
let newCards = 0;
let totalSum;
let totalCards;
let sum = 0;
let message;





function startGame(){
    messageEl.classList.add("message-el");
    firstCard = Math.round(Math.random()*21);
    secondCard = Math.round(Math.random()*21);

    sum = firstCard + secondCard;
    totalSum = "Sum: "+ sum;
    totalCards = "Cards: " + firstCard + " , " + secondCard;

    let hasBlackJack = false;
    let isAlive = true;

    if (sum < 21){
        message = "Do you want to draw a new card?";
        document.getElementById("new-card-btn").style.display = "initial";
    } else if (sum === 21){
        message = "Congrats! you've got BlackJack.";
        hasBlackJack = true;
        document.getElementById("start-btn").style.display = "none";
        document.getElementById("new-card-btn").style.display = "none";
        document.getElementById("new-game-btn").style.display = "initial";
    } else {
        message = "You are out of games!";
        isAlive = false;
        document.getElementById("start-btn").style.display = "none";
        document.getElementById("new-card-btn").style.display = "none";
        document.getElementById("new-game-btn").style.display = "initial";
    }
    
    messageEl.innerText = (message);
    sumEl.textContent = (totalSum);
    cardEl.textContent = (totalCards);
}






function newGame(){
    messageEl.classList.remove("message-el");
    totalSum = "Sum: "
    totalCards = "Cards: "
    message = "Do you want to play?"
    messageEl.innerText = (message);
    sumEl.textContent = (totalSum);
    cardEl.textContent = (totalCards);
    document.getElementById("start-btn").style.display = "initial";
    document.getElementById("new-game-btn").style.display = "none";

}

function newCard(){
    newCards = Math.round(Math.random()*21);
    sum += newCards;
    totalSum = "Sum: "+ sum;
    totalCards += " , " + newCards;
    if (sum < 21){
        message = "Do you want to draw a new card?";
        document.getElementById("new-card-btn").style.display = "initial";
    } else if (sum === 21){
        message = "Congrats! you've got BlackJack.";
        hasBlackJack = true;
        document.getElementById("start-btn").style.display = "none";
        document.getElementById("new-card-btn").style.display = "none";
        document.getElementById("new-game-btn").style.display = "initial";
    } else {
        message = "You are out of games!";
        isAlive = false;
        document.getElementById("start-btn").style.display = "none";
        document.getElementById("new-card-btn").style.display = "none";
        document.getElementById("new-game-btn").style.display = "initial";
    }

    messageEl.innerText = (message);
    sumEl.textContent = (totalSum);
    cardEl.textContent = (totalCards);
}