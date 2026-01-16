var previousUserList = document.querySelector(".previousUserList");
var scores = [];

function init() {
    var storedScores = JSON.parse(localStorage.getItem("leaderBoard"));
    
    if (storedScores != null) {
        scores = storedScores;
    }
    displayScore();
};

function displayScore() {
    for (var i = 0; i < scores.length; i++) {
        var allLeaderBoard = scores[i];
        var listItem = document.createElement("li");
        listItem.textContent = "Username: " + allLeaderBoard.user + " Score: " + allLeaderBoard.score + " Time Left: " + allLeaderBoard.timeLeft + " seconds ";
        previousUserList.appendChild(listItem);
    }
};
init();