"use script";
let MyNumber = Math.trunc(Math.random() * 20) + 1;
let score = 7;
let HS = 0;

document.querySelector("#btnCheck").addEventListener("click", function () {
  const guess = Number(document.querySelector("#inputBox").value);
  if (!guess) {
    document.querySelector("#startGuess").textContent = "⛔No Number";
  } else if (guess > MyNumber) {
    if (score > 1) {
      document.querySelector("#startGuess").textContent = "📈 High";
      score--;
      document.querySelector("#actualScore").textContent = score;
    } else {
      document.querySelector("#startGuess").textContent = "❌ Game Over";
      document.querySelector("#actualScore").textContent = 0;
      document.querySelector("body").style.backgroundColor = "#F25B5B";
    }
  } else if (guess < MyNumber) {
    if (score > 1) {
      document.querySelector("#startGuess").textContent = "📉 Low";
      score--;
      document.querySelector("#actualScore").textContent = score;
    } else {
      document.querySelector("#startGuess").textContent = "❌ Game Over";
      document.querySelector("#actualScore").textContent = 0;
      document.querySelector("body").style.backgroundColor = "#F25B5B";
    }
  } else if (guess === MyNumber) {
    document.querySelector("#startGuess").textContent = "🥂Correct Number";
    document.querySelector("#box").textContent = MyNumber;
    document.querySelector("body").style.backgroundColor = "#B1F653";
    if (score > HS) {
      HS = score;
      document.querySelector("#actualHighScore").textContent = HS;
    }
  }
});

document.querySelector("#btnPlayAgain").addEventListener("click", function () {
  score = 7;
  MyNumber = Math.trunc(Math.random() * 20) + 1;
  document.querySelector("#startGuess").textContent = "Start guessing...";
  document.querySelector("#actualScore").textContent = score;
  document.querySelector("#box").textContent = "?";
  document.querySelector("#inputBox").value = "";
  document.querySelector("body").style.backgroundColor = "lightgray";
});