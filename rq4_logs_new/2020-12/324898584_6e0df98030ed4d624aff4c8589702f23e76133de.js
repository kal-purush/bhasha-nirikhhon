const startButton = document.getElementById('start-btn')
const sounds = ["applause", "boo", "gasp", "tada", "victory", "wrong", "anthem"];

const body = document.body
const nextButton = document.getElementById('next-btn')
const questionContainerElement = document.getElementById('question-container')
const questionElement = document.getElementById('question')
const answerButtonsElement = document.getElementById('answer-buttons')

startButton.addEventListener('click', startGame)
nextButton.addEventListener('click', () => {
    currentQuestionIndex++
    setNextQuestion()
})
let shufledQuestions, currentQuestionIndex


function startGame() {
    startButton.classList.add('hide')
    questionContainerElement.classList.remove('hide')
    shufledQuestions = questions.sort(() => Math.random() -0.5)
    currentQuestionIndex = 0
    setNextQuestion()
}

function setNextQuestion() {
    resetState()
   showQuestion(shufledQuestions[currentQuestionIndex]) 
}
function showQuestion(question) {
    questionElement.innerText = question.question;
    question.answers.forEach(answer => {
        const button = document.createElement('button')
        button.classList.add('btn')
        button.innerText = answer.text
        if (answer.correct){
            button.dataset.correct = answer.correct
        }
        button.addEventListener('click', selectAnswer)
        answerButtonsElement.appendChild(button)
    });
}

function resetState() {
    clearStatusClass(document.body)
    nextButton.classList.add('hide')
    while(answerButtonsElement.firstChild) {
    answerButtonsElement.removeChild(answerButtonsElement.firstChild)
}
}
function selectAnswer(e) {
    const selectedButton = e.target
    const correct = selectedButton.dataset.correct
    setStatusClass(document.body, correct)
    Array.from(answerButtonsElement.children).forEach(button => {
        setStatusClass(button, button.dataset.correct)
    })
    if (shufledQuestions.length > currentQuestionIndex + 1) {
        nextButton.classList.remove('hide')
    } else {
        document.getElementById('anthem').play();
        startButton.innerText = 'restart'
        startButton.classList.remove('hide')
    }
    
}

function setStatusClass(element, correct) {
    clearStatusClass(element)
    if (correct) {
        element.classList.add('correct')
        document.getElementById('victory').play();
        
    } else {
        element.classList.add('wrong')
        document.getElementById('wrong').play();
        
    }
}
function clearStatusClass(element) {
    element.classList.remove('correct')
    element.classList.remove('wrong')
}

const questions = [
    {
        question: "what is 2 + 2",
        answers: [
            {text: '4', correct: true},
            {text: '5', correct: false},
            {text: '6', correct: false},
            {text: '2', correct: false},
        ]
    },
    {
        question: "what is 1 + 2",
        answers: [
            {text: '4', correct: false},
            {text: '5', correct: false},
            {text: '6', correct: false},
            {text: '2', correct: false},
        ]
    },
    {
        question: "what is 3 + 2",
        answers: [
            {text: '4', correct: false},
            {text: '5', correct: true},
            {text: '6', correct: false},
            {text: '2', correct: false},
        ]
    }
]