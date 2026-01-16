let question = [];
let currentQuestion = "";
let correctCount, missedCount, startTime, timer, questionNum, totalQuestions, finalScore;


function gameInit() {
    questions = [
        {
            question: "What Is The State Capital Of New Mexico?",
            answerCorrect: "Santa Fe",
            answers: [
                "Albuquerque",
                "Santa Fe",
                "Las Cruces",
                "Portland"
            ]
        },
        {
            question: "What Is The Country With The Highest Population?",
            answerCorrect: "China",
            answers: [
                "India",
                "United States",
                "China",
                "Mexico"
            ]
        },
        {
            question: "What Is The Smallest U.S. State?",
            answerCorrect: "Rhode Island",
            answers: [
                "Deleware",
                "Rhode Island",
                "New Hampshire",
                "Hawaii"
            ]
        },
        {
            question: "How Many U.S. States Border Mexico?",
            answerCorrect: "Four",
            answers: [
                "Six",
                "Three",
                "Four",
                "Five"
            ]
        },
        {
            question: "Which U.S. State Is Known As The Garden State?",
            answerCorrect: "New Jersey",
            answers: [
                "Vermont",
                "Florida",
                "New Jersey",
                "Oregon"
            ]
        },
        {
            question: "What Percentage Of The Earth's Oxygen Is Produced By The Amazon Rainforest?",
            answerCorrect: "20%",
            answers: [
                "15%",
                "30%",
                "8%",
                "20%"
            ]
        },
        {
            question: "Which Country Has The Most Natural Lakes?",
            answerCorrect: "Canada",
            answers: [
                "Canada",
                "United States",
                "Brazil",
                "Russia"
            ]
        },
        {
            question: "How Many Countries Have A Coast On Russia?",
            answerCorrect: "Three",
            answers: [
                "Two",
                "One",
                "Three",
                "Four"
            ]
        },
        {
            question: "WHich Is The Greenest Country In The World?",
            answerCorrect: "Finland",
            answers: [
                "Finland",
                "Norway",
                "Singapore",
                "Switzerland"
            ]
        },
        {
            question: "How Many Roads In Canada Are Unpaved?",
            answerCorrect: "75%",
            answers: [
                "50%",
                "15%",
                "75%",
                "63%"
            ]
        }
    ];
    missedCount = 0;
    correctCount = 0;
    questionNum = 1;
    currentQuestion = randQuestion();
    totalQuestions = questions.length + 1;
    setQuestion();
    startTime = 3;
    $('.current-question').html(questionNum);
    $('.total-questions').html(totalQuestions);
    $('.time').html(startTime);
    timer = setInterval(myTimer, 1000);
}

function myTimer() {
    startTime--;
    $('.time').html(startTime);
    console.log(startTime);

    if (startTime <= 0) {
        changeQuestion();
        missedCount++;
    }

    if (questions.length <= 0) {
        $('.game-body').addClass('invisible');
        $('.game-over').removeClass('invisible');
        clearInterval(timer);
        calcScore();
    }

}

function randQuestion() {
    let qIndex = Math.floor(Math.random()*questions.length);
    let q = questions[qIndex];
    console.log("index of question " + qIndex);
    questions.splice(qIndex,1);
    console.log("Questions Left: " + questions.length);
    return q;
}

function setQuestion() {
    $('.question-text').html(currentQuestion.question);
    setAnswer();
}

function setAnswer() {
    a = currentQuestion.answers.length;
    for (let i = 0; i < a; i++) {
        let ansIndex = Math.ceil(Math.random()*currentQuestion.answers.length) - 1;
        let ansLocation = "#answer-" + (i + 1);
        let ans = currentQuestion.answers[ansIndex];
        $(ansLocation).html(ans);
        currentQuestion.answers.splice(ansIndex, 1);
    }
}

function changeQuestion() {
    currentQuestion = randQuestion();
    setQuestion();
    clearInterval(timer);
    startTime = 3;
    questionNum++;
    $('.time').html(startTime);
    $('.current-question').html(questionNum);
    timer = setInterval(myTimer, 1000);
    $('.time').toggle().toggle();
}

function calcScore() {
    finalScore = (correctCount / totalQuestions) * 100;
    console.log(finalScore);
    $('.grade').html(finalScore);
}





$('.btn-init').click(function (e) { 
    e.preventDefault();
    $('.game-body').removeClass('invisible');
    $('.game-init').addClass('invisible');
    $('.title').addClass('move-left');
    $('.move-left').css({
        'animation': 'bounceOutLeft 2s linear 0s 1 normal none',
        'animation-fill-mode': 'forwards'
    });
    gameInit();
    
});

$('.answer-text').click(function (e) { 
    e.preventDefault();
    let answerClicked = $(this).text();
    if (answerClicked == currentQuestion.answerCorrect) {
        correctCount++;
    } else {
        missedCount++;
    }

    if (questions.length > 0) {
        changeQuestion();
    } else {
        $('.game-body').addClass('invisible');
        $('.game-over').removeClass('invisible');
        calcScore();
    }

});


$('.reset').click(function (e) { 
    e.preventDefault();
    $('.game-over').addClass('invisible');
    $('.game-init').removeClass('invisible');
    $('.move-left').css({'animation-fill-mode': 'none'});
    // $('.title').removeClass('.move-left');
    $('.move-left').toggle().toggle().css({
        'left': '10.5rem'
    });


    gameInit();
});