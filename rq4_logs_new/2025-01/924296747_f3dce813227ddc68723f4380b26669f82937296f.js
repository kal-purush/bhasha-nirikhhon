const startButton = document.getElementById('start-btn');
const questionContainerElement = document.getElementById('question-container');
const questionElement = document.getElementById('question');
const answerButtonsElement = document.getElementById('answer-buttons');
const timerElement = document.getElementById('time');
const endContainerElement = document.getElementById('end-container');
const finalScoreElement = document.getElementById('final-score');
const scoreForm = document.getElementById('score-form');
const initialsInput = document.getElementById('initials');

let shuffledQuestions, currentQuestionIndex;
let time = 60;
let timerInterval;

const questions = [
    {
        question: "What does API stand for?",
        answers: [
            { text: "Application Programming Interface", correct: true },
            { text: "Application Program Internet", correct: false },
            { text: "Application Programming Integration", correct: false },
            { text: "Application Program Interface", correct: false }
        ]
    },
    {
        question: "Which HTTP method is used to create a resource?",
        answers: [
            { text: "GET", correct: false },
            { text: "POST", correct: true },
            { text: "PUT", correct: false },
            { text: "DELETE", correct: false }
        ]
    },
    // Add more questions as needed
];

startButton.addEventListener('click', startGame);
scoreForm.addEventListener('submit', saveScore);

function startGame() {
    startButton.classList.add('hide');
    shuffledQuestions = questions.sort(() => Math.random() - 0.5);
    currentQuestionIndex = 0;
    questionContainerElement.classList.remove('hide');
    startTimer();
    setNextQuestion();
}

function startTimer() {
    timerInterval = setInterval(() => {
        time--;
        timerElement.textContent = time;
        if (time <= 0) {
            endGame();
        }
    }, 1000);
}

function setNextQuestion() {
    resetState();
    showQuestion(shuffledQuestions[currentQuestionIndex]);
}

function showQuestion(question) {
    questionElement.textContent = question.question;
    question.answers.forEach(answer => {
        const button = document.createElement('button');
        button.textContent = answer.text;
        button.classList.add('btn');
        if (answer.correct) {
            button.dataset.correct = answer.correct;
        }
        button.addEventListener('click', selectAnswer);
        answerButtonsElement.appendChild(button);
    });
}

function resetState() {
    answerButtonsElement.innerHTML = '';
}

function selectAnswer(e) {
    const selectedButton = e.target;
    const correct = selectedButton.dataset.correct;
    if (!correct) {
        time -= 10;
        if (time < 0) time = 0;
        timerElement.textContent = time;
    }
    currentQuestionIndex++;
    if (currentQuestionIndex < shuffledQuestions.length) {
        setNextQuestion();
    } else {
        endGame();
    }
}

function endGame() {
    clearInterval(timerInterval);
    questionContainerElement.classList.add('hide');
    endContainerElement.classList.remove('hide');
    finalScoreElement.textContent = time;
}

function saveScore(e) {
    e.preventDefault();
    const initials = initialsInput.value.trim();
    if (initials) {
        const highScores = JSON.parse(localStorage.getItem('highScores')) || [];
        const newScore = { score: time, initials };
        highScores.push(newScore);
        localStorage.setItem('highScores', JSON.stringify(highScores));
        initialsInput.value = '';
        alert('Score saved!');
    }
}