$(document).ready(function () {

    // start game
    $("#start_button").on("click", function () {
        $("#start_page").hide();
        showQuestion();
    });

    // Get question and answer choices to display
    function showQuestion() {
        $("#question_display").text(triviaQ[currentQuestion].question)
        $("#choices_col").empty();
        for (var i = 0; i < triviaQ[currentQuestion].choices.length; i++) {
            var answer = triviaQ[currentQuestion].choices[i]
            var button = $("<input type='radio' class='radio' name='answers' value=' " + answer + " '>");
            button.on("click", answerClick)

            $("#choices_col").append(button);
            $("#choices_col").append(answer);
        }

        // Timer        
        timer = 10;
        intervalId = setInterval(function () {
            var correctAnswerIndex = triviaQ[currentQuestion].correct;
            var correctAnswer = triviaQ[currentQuestion].choices[correctAnswerIndex]
            showAnswer(correctAnswer);
        }, 10000);
        // $("#timer").append("<progress value='0' max='10'>");
    }

    function showAnswer(correctAnswer, userChoice) {
        var imgsrc = triviaQ[currentQuestion].image
        $("#choices_col").html("<br>" + correctAnswer + "<br>");
        var img = $("<br><img>")
        img.attr("src", imgsrc);
        $("#choices_col").append(img);
        $("#timer").empty();
        $("#question_display").empty();
        if (!userChoice) {
            unanswered++;
            $("#question_display").text(youWrong)
        } else if (correctAnswer === userChoice) {
            numCorrect++;
            $("#question_display").text(youRight)
        } else {
            numWrong++;
            $("#question_display").text(youWrong)
        }
        currentQuestion++;
        clearInterval(intervalId);
        if (triviaQ.length === currentQuestion) {
            setTimeout(gameEnd, 7000);
        } else {
            setTimeout(showQuestion, 7000);
        }
    }

    var youRight = "You're right!";
    var youWrong = "The correct answer is:";

    function answerClick() {
        var userChoice = $(this).val().trim();
        var correctAnswer = triviaQ[currentQuestion].choices[triviaQ[currentQuestion].correct];
        showAnswer(correctAnswer, userChoice);
    }

    function gameEnd() {
        var unansweredP = $("<p>").text("Unanswered: " + unanswered);
        var correctP = $("<p>").text("Correct: " + numCorrect);
        var wrongP = $("<p>").text("Incorrect: " + numWrong);
        $("#answer_display").empty();
        $("#question_display").empty();
        $("#answer_display").append(unansweredP);
        $("#answer_display").append(correctP);
        $("#answer_display").append(wrongP);


    }

    var unanswered = 0;
    var numCorrect = 0;
    var numWrong = 0;
    var currentQuestion = 0;
    var timer;
    var timeleft;

    // trivia question array
    var triviaQ = [{
        question: "What type of galaxy is the most common in the universe?",
        choices: ["Spiral", "Elliptical", "Irregular", "Lenticular"],
        correct: 1,
        image: './assets/images/ellipticalgalaxy.gif'
    }, {
        question: "What is the coldest place in the universe?",
        choices: ["Virgo Cluster", "RXJ1347 Cluster", "The Boomerang Nebula", "My Heart"],
        correct: 2,
        image: './assets/images/cold.gif'
    }, {
        question: "What is the hottest place in the universe?",
        choices: ["Virgo Cluster", "RXJ1347 Cluster", "The Boomerang Nebula", "The Milky Way"],
        correct: 0,
        image: './assets/images/thatshot.gif'
    }, {
        question: "What is the most common type of star found in the Milky Way?",
        choices: ["Blue Giant", "Neutron Star", "Protostar", "Red Dwarf"],
        correct: 3,
        image: 'https://media.giphy.com/media/ctGFLebG1AqK4/giphy.gif'
    }, {
        question: "The point thought to represent the center of a black hole is known as a:",
        choices: ["Singularity", "Infinitum", "Event Horizon", "Ergosphere"],
        correct: 0,
        image: './assets/images/blackhole.gif'
    }, {
        question: "The overall lifespan of a star is determined by its:",
        choices: ["Chemical Composition", "Mass", "Temperature", "Luminosity"],
        correct: 1,
        image: 'https://media.giphy.com/media/SWflBO9boCA9i/giphy.gif'
    }, {
        question: "The distribution of matter between a distant light source and an observer, that is capable of bending the light from the source as the light travels towards the observer is called what?",
        choices: ["Intraocular Lens", "Gravity", "Spyglass", "Gravitational Lens"],
        correct: 3,
        image: 'https://media.giphy.com/media/gcmg6s0p72hDa/giphy.gif'
    }, {
        question: "What does NASA stand for?",
        choices: ["National Aeronautics and Space Administration", "Nationally Accomplished Spacepeople and Astronauts", "National Appreciation for Space and Astronomy", "National Aeronautics and Space Act"],
        correct: 0,
        image: './assets/images/astronaut.gif'
    }, {
        question: "What is a planet called that orbits a star outside the solar system?",
        choices: ["Rogue Planet", "Exoplanet", "Binary Planet", "Jovian Planet"],
        correct: 1,
        image: 'https://media.giphy.com/media/YA2bZh31eFXi0/giphy.gif'
    }, {
        question: "How many light years away is Andromeda?",
        choices: ["4.735", "1.753", "2.537", "3.237"],
        correct: 2,
        image: 'https://media.giphy.com/media/3vvUe9b8iaI5a/giphy.gif'
    }];
});