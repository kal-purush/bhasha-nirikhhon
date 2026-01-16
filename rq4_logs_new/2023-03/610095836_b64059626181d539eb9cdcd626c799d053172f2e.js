const myQuestions = [
    {
        question: 'What are the six primitive data types in JavaScript?',
        choices: ['string, number, boolean, bigInt, symbol, undefined', 
        'string, num, falsy, bigInt, symbol, undefined',
        'sentence, int, truthy, bigInt, symbol, undefined',
        'sentence, float, data, bigInt, symbol, undefined'],
        correctAnswer: 0,
    },
    {
        question: 'What are the two types of scope JavaScript uses?',
        choices: ['Global and Local','Surrounding and Inner','Inside and Outside','Abroad and Local'],
        correctAnswer: 0,
    },
    {
        question: "From the given array which index is the letter 'c' on? ['a', 'b', 'c', 'd']",
        choices: ['0','1','2','3'],
        correctAnswer: 2,
    },
    {
        question: 'What operator is used to assign a value to a declared variable?',
        choices: ['Double-equal (==)','Question mark (?)','Colon (:)','Equal sign (=)'],
        correctAnswer: 3,
    },
    {
        question: 'Inside the HTML document, where do you place your JavaScript code?',
        choices: ['In the <footer> element','Inside the <script> element','Inside the <link> element','Inside the <head> element'],
        correctAnswer: 1,
    },
    {
        question: 'What type of data is stored in quotes?',
        choices: ['Array','Boolean','String','Number'],
        correctAnswer: 2,
    },
    {
        question: 'What does DOM stand for?',
        choices: ['Document Obeject Model','Document Object Module','Document Overview Monitor','Document Organizing Management'],
        correctAnswer: 0,
    },
    {
        question: 'What does JSON stand for?',
        choices: ['JavaScript Object Navigation','JavaScript Object Notation','JavaScript Oriented Navigation','JavaScript Organized Notation'],
        correctAnswer: 1,
    },
    {
        question: 'What is an object method?',
        choices: ['A function associated with an object','An array saved inside of an object','Keys in an object that have a number assigned to it','A function that takes an object for an argument'],
        correctAnswer: 2,
    },
    {
        question: 'How do we declare a conditional statement in JavaScript?',
        choices: ['if...else','for loop','while loop','difference...between'],
        correctAnswer: 0,
    }
  ]

const quizContainer = document.getElementById('quiz-container');
const startButton = document.getElementById('start');
let currentQuestion = 0;
let timeLeft = 300;
let timerInterval;

// Function to start the quiz
function startQuiz() {
  // Hide start button and show quiz container
  document.querySelector('.container-fluid.text-center').style.display = 'none';
  quizContainer.style.display = 'block';

  // Start timer
  startTimer();

  // Display first question
  displayQuestion();
}

// Function to start the timer
function startTimer() {
  timerInterval = setInterval(function() {
    timeLeft--;
    document.querySelector('.timer-count').textContent = timeLeft;
    if (timeLeft <= 0) {
      clearInterval(timerInterval);
      endQuiz();
    }
  }, 1000);
}

// Function to display a question
function displayQuestion() {
  
}
startButton.addEventListener("click", startQuiz)