var randomDiceNumber1 = Math.floor(Math.random() * 6) + 1; //1-6

var randomDiceImage = "dice" + randomDiceNumber1 + ".png"; //dice1.png - dice6.png

var randomImageSource = "images/" + randomDiceImage; //images/dice1.png - images/dice6.png

var image1 = document.querySelectorAll("img")[0];

image1.setAttribute("src", randomImageSource);

var randomDiceNumber2 = Math.floor(Math.random() * 6) + 1;

var randomImageSource2 = "images/dice" + randomDiceNumber2 + ".png";

var image2 = document.querySelectorAll("img")[1];

image2.setAttribute("src", randomImageSource2);


//If player 1 wins
if (randomDiceNumber1 > randomDiceNumber2) {
  document.querySelector("h1").innerHTML = "🚩 Play 1 Wins!"; }

else if (randomDiceNumber2 > randomDiceNumber1) {
  document.querySelector("h1").innerHTML = "Player 2 Wins! 🚩"; }

else {
  document.querySelector("h1").innerHTML = "Draw!"; }