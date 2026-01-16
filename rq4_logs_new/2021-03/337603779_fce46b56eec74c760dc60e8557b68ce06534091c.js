//Global variables defined
let fiveQuestions = [["Commonly used data types DO NOT include:", "1. strings", "2. booleans", "3. alerts", "4. numbers"], 
["The condition in an if / else statement is enclosed within ___.", "1. quotes", "2. curly brackets", "3. parentheses", "4. square brackets"], 
["Arrays in javascript can be used to store ___.", "1. numbers and strings", "2. other arrays", "3. booleans", "4. all of the above"], 
["String values must be enclosed within ___ when being assigned to variables.", "1. commas", "2. curly brackets", "3. quotes", "4. parentheses"], 
["A very useful tool used during development and debugging for printing content to the debugger is:", "1. JavaScript", "2. terminal / bash", "3. for loops", "4. console.log"]];

let i = 0;
let correct = 0;
let incorrect = 0;
let scores = [];
let alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
let bruh = true;

let body = document.querySelector("body");
let header = document.querySelector("header");
let bold = document.querySelector("#bold");
let content = document.querySelector("#content");
let div = document.querySelector("div");
let button = document.querySelector(".button");
let highScoresBtn = document.querySelector("#highscores");

let submit = document.createElement("button");
let input = document.createElement("input");
let result = document.createElement("p");
let back = document.createElement("button");
let clear = document.createElement("button");
let footer = document.createElement("footer");

let timer = document.querySelector("#time");
let timeLeft = 75;
let timeInterval;

//Function that populates the #bold and #content sections with each question
function press() {
    let answers = ["3", "3", "4", "3", "4"];
    if (this.innerHTML !== "Start Quiz") {
        if (this.innerHTML.includes(answers[i])) {
            correct++;
            result.innerHTML = "Correct!";
        }
        else {
            incorrect++;
            result.innerHTML = "Wrong!";
            if (timeLeft > 10) {
                timeLeft = timeLeft - 10;
            }
            else {
                timeLeft = 0;
            }
        }
        i++;
        footer.innerHTML = "";
        footer.appendChild(result);
        body.appendChild(footer);

    }
    if (i < fiveQuestions.length) {
        bold.innerHTML = "";
        let question = document.createElement("h1");
        question.innerHTML = fiveQuestions[i][0];
        bold.appendChild(question);
        content.innerHTML = "";

        for (j = 1; j <= fiveQuestions.length - 1; j++) {
            let one = document.createElement("button");
            one.classList.add("button");
            one.innerHTML = fiveQuestions[i][j];
            content.appendChild(one);
            one.addEventListener("click", press);
        }
    }
    else {
        allDone();
    }
}

//Function that displays the completion screen when the quiz is finished
function allDone() {
    clearInterval(timeInterval);
    timer.innerHTML = "Time: " + timeLeft;
    bold.innerHTML = "";
    let finish = document.createElement("h1");
    finish.innerHTML = "All done!";
    bold.appendChild(finish);

    content.innerHTML = "";
    let score = document.createElement("h2");
    score.innerHTML = "Your final score is " + timeLeft + ".";
    content.appendChild(score);

    div.innerHTML = "";
    
    let initials = document.createElement("p");
    initials.innerHTML = "Enter initials:";
    div.appendChild(initials);

    div.appendChild(input);

    submit.classList.add("submit");
    submit.innerHTML = "Submit";
    div.appendChild(submit);

    content.appendChild(div);
}

//Function that submits the user's score and runs the highScores function
submit.addEventListener("click", function() {
    scores.push([input.value, timeLeft]);
    highScores();
});
// submit.addEventListener("click", submitIt);
// function submitIt() {
//     footer.remove();
//     let fin = input.value.toUpperCase();
    

//     for (i in fin) {
//         if (alphabet.includes(i)) {
//             bruh = false;
//             console.log(fin + " false");
//         }
//     }
//     if (fin.length !== 2) {
//         bruh = false;
//     }

//     if (bruh === true) {
//         console.log(fin + " true");
        
//         scores.push([fin, timeLeft]);
//         highScores();
//     }
//     else {
//             let error = document.createElement("h2");
//             error.innerHTML = "Input must be two letters";
//             footer.innerHTML = "";
//             footer.appendChild(error);
//             body.appendChild(footer);
//             allDone();
//     }
//     // scores.push([input.value, timeLeft]);
    
// }

//Function that displays a list of all the recorded high scores
function highScores() {
    header.remove();
    footer.remove();
    clearInterval(timeInterval);
    timeLeft = 0;
    // timer.innerHTML = "";
    i = 0;
    correct = 0;
    incorrect = 0;
    // highScoresBtn.innerHTML = "";
    content.innerHTML = "";
    div.innerHTML = "";
    bold.innerHTML = "";

    let intro = document.createElement("h1");
    intro.innerHTML = "Highscores";
    bold.appendChild(intro);

    for (j in scores) {
        let hs = document.createElement("p");
        hs.innerHTML = scores[j][0] + " - " + scores[j][1];
        content.appendChild(hs);
    }
    
    content.appendChild(div);

    back.innerHTML = "Go Back";
    div.appendChild(back);

    clear.innerHTML = "Clear Highscores";
    div.appendChild(clear);
}

//Button function that takes the user back to the home screen
back.addEventListener("click", function() {
    body.appendChild(header);
    timeLeft = 0;
    timer.innerHTML = "Time: " + timeLeft;

    bold.remove();
    let intro = document.createElement("h1");
    intro.innerHTML = "Coding Quiz Challenge";
    bold.innerHTML = "";
    bold.appendChild(intro);
    body.appendChild(bold);

    content.remove();
    let directions = document.createElement("h2");
    directions.innerHTML = "Try to answer the following code-related questions within the time limit. Keep in mind that incorrect answers will penalize your scoreTime by ten seconds!";
    content.innerHTML = "";
    content.appendChild(directions);

    div.innerHTML = "";
    div.appendChild(button);
    content.appendChild(div);

    body.appendChild(content);    
});

//A series of functions for the .button button that starts the quiz and cycles through each question
button.addEventListener("click", press);
button.addEventListener("click", function() {
    timeLeft = 75;
    timer.innerHTML = "Time: " + timeLeft;
})
button.addEventListener("click", function() {
    // timer.innerHTML = "Time: " + timeLeft;

    timeInterval = setInterval(function () {
        if (timeLeft > 0) {
            timeLeft--;
            timer.innerHTML = "Time: " + timeLeft;
        }
        if (timeLeft === 0) {
            allDone();
        }
    }, 1000);
});

//A button that runs the highScores function
highScoresBtn.addEventListener("click", highScores);

//A function that displays the number of questions correct and incorrect when the input field is clicked
input.addEventListener("click", function() {
    result.innerHTML = "Correct: " + correct + " Incorrect: " + incorrect;
})

//A button function that clears the highscores page
clear.addEventListener("click", function() {
    scores = [];
    content.innerHTML = "";

    let div = document.createElement("div");
    content.appendChild(div);

    back.innerHTML = "Go Back";
    div.appendChild(back);

    clear.innerHTML = "Clear Highscores";
    div.appendChild(clear);
})