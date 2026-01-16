var h1 = document.querySelector("h1");

var randomNumber1 = Math.floor(Math.random() * 6) + 1;
document.querySelector(".img1").setAttribute("src", "./images/dice" + randomNumber1 + ".png");
var randomNumber2 = Math.floor(Math.random() * 6) + 1;
document.querySelector(".img2").setAttribute("src", "./images/dice" + randomNumber2 + ".png");

if (randomNumber1 > randomNumber2) {
    h1.textContent = "🚩 Player 1 Wins!";
} else if (randomNumber1 < randomNumber2) {
    h1.textContent = "Player 2 Wins! 🚩";
} else {
    h1.textContent = "Draw!";
}