/* global $ */
/* exports isGameOver, whoWon, playTurn, restart, currentQuestion, correctAnswer, numberOfAnswers */

// A constructor function allows us to easily make question objects. Other methods include using arrays etc.
function Question (prompt, answers, answerIndex) {
  this.prompt = prompt
  this.answer = answers
  this.correctAns = answerIndex
}

// using the new keyword and the constructor we can create questions for the quiz
var qn1 = new Question('How long is an eon?', ['1 million years', '1 thousand years', '1 billion years', '1 hundred years'], 2)
var qn2 = new Question("What was Charlie Brown's father's job?", ['Barber', 'Plumber', 'Accountant', 'Mechanic'], 0)
var qn3 = new Question('Which planet spins the fastest?', ['Earth', 'Mars', 'Saturn', 'Jupiter'], 3)
var qn4 = new Question('When did the Beatles win their first Grammy?', ['1963', '1965', '1966', '1964'], 1)
var qn5 = new Question('How many staircases does Hogwarts have?', ['104', '138', '142', '79'], 2)
var qn6 = new Question('Which species stole the plans to the Death Star?', ['Bothans', 'Wookies', 'Ewoks', 'Jawas'], 0)
// we can create an object to represent all of the settings and scores for the quiz
var quiz = {
  currentQuestion: 0,
  questions: [qn1, qn2, qn3, qn4, qn5, qn6],
  isGameOver: false,
  player1: 0,
  player2: 0
}

// numberOfQuestions should return an integer that is the number of questions in a game
function numberOfQuestions () {
  return quiz.questions.length
}

// currentQuestion should return an integer that is the zero-based index of the current question in the quiz
function currentQuestion () {
  return quiz.currentQuestion
}

// correctAnswer should return an integer that is the zero-based index the correct answer for the currrent question
function correctAnswer () {
  return quiz.questions[quiz.currentQuestion].correctAns
}

// numberOfAnswers should return an integer that is the number of choices for the current question
function numberOfAnswers () {
  console.log("num of ans" + quiz.questions[quiz.currentQuestion].answer.length)
  return quiz.questions[quiz.currentQuestion].answer.length
}

// playTurn should take a single integer, which specifies which choice the current player wants to make. It should return a boolean true/false if the answer is correct.
function playTurn (choice) {
  if (quiz.isGameOver === true) {
    return false
  }
  var correct = false
  if (choice === quiz.questions[quiz.currentQuestion].correctAns) {
    correct = true
    if ((quiz.currentQuestion % 2) === 0) {
      quiz.player1++
    } else if ((quiz.currentQuestion % 2) === 1) {
      quiz.player2++
    }
  }
  quiz.currentQuestion++
  if (quiz.currentQuestion === numberOfQuestions()) {
    quiz.isGameOver = true
  }
  return correct
}

// isGameOver should return a true or false if the quiz is over.
function isGameOver () {
  return quiz.isGameOver
}

// whoWon should return 0 if the game is not yet finished, 1 or 2 depending on which player won, else 3 if the game is a draw.
function whoWon () {
  if (quiz.isGameOver === false) return 0
  if (quiz.player1 > quiz.player2) return 1
  if (quiz.player1 < quiz.player2) return 2
  return 3
}

// restart should restart the game so it can be played again.
function restart () {
  quiz.currentQuestion = 0
  quiz.isGameOver = false
  quiz.player1 = 0
  quiz.player2 = 0
}

// a function to update the display whenever the data changes
function updateDisplay () {
  if (isGameOver()) {
     if(whoWon() === 1) {
       $('.notice').text('Player 1 wins.')
     } else if(whoWon() === 2) {
       $('.notice').text('Player 2 wins.')
     } else if(whoWon() === 3) {
       $('.notice').text('It is a tie!')
     }
  } else {
    $('.question').text((quiz.currentQuestion + 1) + ') ' + quiz.questions[quiz.currentQuestion].prompt)
    // hard coded display, only has 4 answers at a time. Each is displayed as a button, so can use the order (eg) that they appear in the dom to select them
    $('#choice1').text(quiz.questions[quiz.currentQuestion].answer[0])
    $('#choice2').text(quiz.questions[quiz.currentQuestion].answer[1])
    $('#choice3').text(quiz.questions[quiz.currentQuestion].answer[2])
    $('#choice4').text(quiz.questions[quiz.currentQuestion].answer[3])
  }
  // update player scores regardless
  $('.score1').text(quiz.player1)
  $('.score2').text(quiz.player2)
}

// the jQuery ready function will add click listeners once the dom is loaded
$(function () {
  $('button').click(function () {
    playTurn($(this).index())
    updateDisplay()
  })
  // update the display for the first time
  updateDisplay()
})

$(function () {
  if (isGameOver()) {
    $('.bottom').appendButton('Replay?')
    $('.reset').addClass('show')
    $('.reset').click(function () {
      restart()
    })
  }
})