
var randomNumber = Math.floor((Math.random()*6) + 1);
var randomNumber2 = Math.floor((Math.random()*6) + 1);

var player1Dice = "images/dice" + randomNumber + ".png";
var player2Dice = "images/dice" + randomNumber2 + ".png";


var image1 = document.querySelectorAll("img")[0];
image1.setAttribute("src", player1Dice);

var image2 = document.querySelectorAll("img")[1];
image2.setAttribute("src", player2Dice);


if( randomNumber > randomNumber2){
    document.querySelector("h1").innerHTML = "Player 1 Wins!";
} else if(randomNumber2 > randomNumber) {
    document.querySelector("h1").innerHTML = "Player 2 Wins!";
} else {
    document.querySelector("h1").innerHTML = "Draw!";
}