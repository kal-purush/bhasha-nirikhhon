
var loadScores = function() {
    var highscores = localStorage.getItem("scores");

    // check if value is null
    if (!highscores) {
        return  error;
    }

    // change stringed storage back to array
    highscores = JSON.parse(localStorage.getItem("scores")) || [];

    // loop through array to print to the page
    for (var i = 0; i < highscores.length; i++) {
        var scoreContainer = document.querySelector(".score-container");

        var scoreSheet = document.createElement("div");
        scoreSheet.className = "score-sheet";

        scoreContainer.appendChild(scoreSheet);

        var pEl = document.createElement("p");
        pEl.textContent = highscores[i].initials + "-" + highscores[i].highscore;

        scoreSheet.appendChild(pEl);
    }
};

loadScores();