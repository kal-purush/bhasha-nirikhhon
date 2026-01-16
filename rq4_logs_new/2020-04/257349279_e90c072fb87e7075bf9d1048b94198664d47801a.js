document.getElementById("startButton").addEventListener("click", function () {
  document.getElementById("quizTitle").innerText = "";
  document.getElementById("quizInstructions").innerText = "";
  document.getElementById("startButton").remove();
  displayQuestion();
  timer = setInterval(function () {
    if (time < 0) {
      clearInterval(timer);
    }
    time--;
    document.getElementById("timeArea").innerText = time;
  }, 1000);
});
var currentQuestion = 0;
var timer;
var time = 60;
function displayQuestion() {
  console.log(currentQuestion);
  if (currentQuestion === 5) {
    clearInterval(timer);
    document.getElementById("questionsArea").innerHTML = "";
    var p = document.createElement("p");
    p.innerText = "Done!";
    var score = document.createElement("p");
    score.innerText = "Your score was " + time;
    document.getElementById("questionsArea").appendChild(p);
    document.getElementById("questionsArea").appendChild(score);
    var inputText = document.createElement("p");
    inputText.innerText = "Enter your initials";
    document.getElementById("questionsArea").appendChild(inputText);
    var input = document.createElement("input");
    var initialsButton = document.createElement("button");
    initialsButton.innerText = "Submit";
    initialsButton.setAttribute("id", "initialsButton");
    document.getElementById("questionsArea").appendChild(initialsButton);

    document.getElementById("questionsArea").appendChild(input);
    return;
  }
  document.getElementById("questionsArea").innerHTML = "";
  var p = document.createElement("p");
  p.innerText = questionArray[currentQuestion].question;
  document.getElementById("questionsArea").appendChild(p);

  for (i = 0; i < 4; i++) {
    var button = document.createElement("button");
    button.innerText = questionArray[currentQuestion].choices[i];
    document.getElementById("questionsArea").appendChild(button);
  }
}
document
  .getElementById("questionsArea")
  .addEventListener("click", function (event) {
    if (event.target.tagName !== "BUTTON") {
      return;
    }

    if (event.target.innerText === "Go Back") {
      window.location.reload();
    }

    if (event.target.innerText === "Clear High Score") {
      localStorage.clear();
      console.log(document.querySelectorAll(".scoreClass"));
      document.getElementById("questionsArea").innerHTML = "";
      document.getElementById("timeArea").innerHTML = "";

      var highScoreText = document.createElement("h2");
      highScoreText.innerText = "High Score";
      document.getElementById("questionsArea").appendChild(highScoreText);
      var goBackButton = document.createElement("button");
      goBackButton.innerText = "Go Back";
      document.getElementById("questionsArea").appendChild(goBackButton);

      var clearHighScoreButton = document.createElement("button");
      clearHighScoreButton.innerText = "Clear High Score";
      document
        .getElementById("questionsArea")
        .appendChild(clearHighScoreButton);

      return;
    }

    if (event.target.innerText === "Submit") {
      var playerInitials = document.getElementsByTagName("input")[0].value;
      console.log(playerInitials);
      var playerScore = time;
      localStorage.setItem(playerInitials, playerScore);
      document.getElementById("questionsArea").innerHTML = "";
      document.getElementById("timeArea").innerHTML = "";

      var highScoreText = document.createElement("h2");
      highScoreText.innerText = "High Score";
      document.getElementById("questionsArea").appendChild(highScoreText);

      for (var i = 0; i < localStorage.length; i++) {
        var key = localStorage.key(i);
        var value = localStorage.getItem(localStorage.key(i));
        console.log(key, value);
        var highScore = document.createElement("p");
        highScore.setAttribute("class", "scoreClass");
        highScore.innerText = key + " " + value;
        document.getElementById("questionsArea").appendChild(highScore);
      }

      var goBackButton = document.createElement("button");
      goBackButton.innerText = "Go Back";
      document.getElementById("questionsArea").appendChild(goBackButton);

      var clearHighScoreButton = document.createElement("button");
      clearHighScoreButton.innerText = "Clear High Score";
      document
        .getElementById("questionsArea")
        .appendChild(clearHighScoreButton);

      console.log("inside");
      return;
    }
    console.log(event);
    var selectedChoice = event.target.innerText;
    console.log(selectedChoice);
    if (questionArray[currentQuestion].answer === selectedChoice) {
      document.getElementById("questionsArea").innerHTML = "";
      var correctDisplay = document.createElement("p");
      correctDisplay.innerText = "Correct";
      document.getElementById("questionsArea").appendChild(correctDisplay);
    } else {
      document.getElementById("questionsArea").innerHTML = "";
      var incorrectDisplay = document.createElement("p");
      incorrectDisplay.innerText = "Incorrect";
      time = time - 5;
      document.getElementById("questionsArea").appendChild(incorrectDisplay);
    }
    currentQuestion++;
    setTimeout(function () {
      displayQuestion();
    }, 1000);
  });

//questions
var questionArray = [
  {
    question: "Commonly used data types DO NOT include:",
    choices: ["strings", "booleans", "alerts", "numbers"],
    answer: "alerts",
  },
  {
    question: "The condition in an if/else staement is enclosed within ______.",
    choices: ["quotes", "curly brackets", "parenthesis", "square brackets"],
    answer: "parenthesis",
  },
  {
    question: "Arrays in JavaScript can be used to store ______.",
    choices: [
      "numbers and strings",
      "other arrays",
      "booleans",
      "all of the above",
    ],
    answer: "all of the above",
  },
  {
    question:
      "String values must be enclosed within ______ when being assigned to variables.",
    choices: ["commas", "curly brackets", "quotes", "parenthesis"],
    answer: "quotes",
  },
  {
    question:
      "A very useful tool used during development and debugging for priniting content to the debugger is:",
    choices: ["JavaScript", "Terminal/bash", "for loops", "console.log"],
    answer: "console.log",
  },
];