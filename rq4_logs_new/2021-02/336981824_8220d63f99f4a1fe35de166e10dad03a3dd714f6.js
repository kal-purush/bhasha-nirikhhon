console.info(performance.navigation.type);
if (performance.navigation.type == performance.navigation.TYPE_RELOAD) { // only runs script when user refreshes
  var randomNumber1 = Math.floor(Math.random() * 6) + 1; // random number between 1 and 6

  var imagePath = "images/dice" + randomNumber1 + ".png"; // images/dice1.png - images/dice6.png

  document.querySelector(".img1").setAttribute("src", imagePath); // load dice image based on random number generated

  var randomNumber2 = Math.floor(Math.random() * 6) + 1;

  imagePath = "images/dice" + randomNumber2 + ".png";

  document.querySelector(".img2").setAttribute("src", imagePath);

  if (randomNumber1 > randomNumber2) {
    document.querySelector("h1").innerHTML = "🚩Player 1 Wins!";
  } else if (randomNumber2 > randomNumber1) {
    document.querySelector("h1").innerHTML = "Player 2 Wins!🚩";
  } else {
    document.querySelector("h1").innerHTML = "Draw!";
  }
}