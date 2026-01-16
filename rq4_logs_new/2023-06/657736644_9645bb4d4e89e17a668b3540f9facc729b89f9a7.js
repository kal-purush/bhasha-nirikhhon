const questions = [ 
    {
    question: "Which of the following is used to read an HTML page and render it?",
    answers: [
        { text: "Web server", correct: false},
        { text: "web network", correct: false},
        { text: "web browser", correct: true},
        { text: "web matrix", correct: false},
        ]
    },
    {
        question: "What is DOM in HTML?",
        answers: [
            { text: "Language dependent application programming", correct: false},
            { text: "Hierarchy of objects in ASP.NET", correct: false},
            { text: "Application programming interface", correct: false},
            { text: "Convention for representing and interacting with objects in html documents", correct: true},
            ] 
    },
    {
    question: "Which of the following is not a HTML5 tag?",
    answers: [
        { text: "track", correct: false},
        { text: "video", correct: false},
        { text: "slider", correct: true},
        { text: "source", correct: false},
        ]
    },
    {
        question: "HTML is a subset of ?",
    answers: [
        { text: " SGMT", correct: false},
        { text: " SGML ", correct: true},
        { text: "SGME", correct: false},
        { text: "XHTML", correct: false},
        ]
    },
    {
        question: "What is the correct syntax of web address?",
    answers: [
        { text: "port://domain.filenmae:path/scheme/prefix ", correct: false},
        { text: "prefix://scheme.port:domain/filename/path", correct: false},
        { text: "path://prefix.port:domain/filename/scheme", correct: false},
        { text: "scheme://prefix.domain:port/path/filename", correct: true},
        ]
    }

];
const questionElement = document.getElementById("question");
const answerButtons = document.getElementById("answer-buttons");
const nextButton = document.getElementById("next-button1");

let currentQuestionIndex = 0;
let score = 0;

function startQuiz(){
    currentQuestionIndex = 0;
    score = 0;
    nextButton.innerHTML = "Next";
    showQuestion();
}
function showQuestion(){
    resetState();
    let currentQuestion = questions[currentQuestionIndex];
    let questionNo = currentQuestionIndex + 1;
    questionElement.innerHTML = questionNo + "." + currentQuestion.question;

    currentQuestion.answers.forEach(answer => {
        const button = document.createElement("button");
        button.innerHTML = answer.text;
        button.classList.add("button1");
        answerButtons.appendChild(button);
        if(answer.correct){
            button.dataset.correct = answer.correct;
        }
        button.addEventListener("click",selectAnswer);
    });
}

function resetState(){
    nextButton.style.display = "none";
    while(answerButtons.firstChild){
        answerButtons.removeChild(answerButtons.firstChild);
    }
}

function selectAnswer(e){
    const selectedBtn = e.target;
    const isCorrect = selectedBtn.dataset.correct == "true";
    if(isCorrect){
        selectedBtn.classList.add("right");
        score++;
    }else{
        selectedBtn.classList.add("wrong");
    }
    Array.from(answerButtons.children).forEach(button => {
        if(button.dataset.correct === "true"){
            button.classList.add("right");
        }
        button.disabled = true;
    });
    nextButton.style.display = "block";
}

function showScore(){
    resetState();
    questionElement.innerHTML = `you scored ${score} out of ${questions.length}!`;
    nextButton.innerHTML = "play again ";
    nextButton.style.display = "block";
}

function handleNextButton(){
    currentQuestionIndex++;
    if(currentQuestionIndex < questions.length){
        showQuestion();
    }else{
        showScore();
    }
}

nextButton.addEventListener("click", ()=>{
    if(currentQuestionIndex < questions.length){
        handleNextButton();
    }else{
        startQuiz();
    }
})

startQuiz();