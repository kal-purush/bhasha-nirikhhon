import "./styles.css";

// use IIFE to protect variable

(function() {
  function Question(question, choices, answer) {
    this.question = question;
    this.choices = choices;
    this.answer = answer;
  }
  Question.prototype.displayQuestion = function() {
    console.log(this.question);
    for (var i = 0; i < this.choices.length; i++) {
      console.log(i + ": " + this.choices[i]);
    }
  };

  Question.prototype.checkAnswer = function(ans) {
    if (ans !== "") {
      if (+ans === this.answer) {
        console.log("Correct");
        points += 10;
      } else {
        console.log("Wrong");
      }
      if (questionIsResponsed < quizz.length) nextQuestion();
      else {
        showResultat();
      }
    }
  };

  var quizz = [
    new Question("Javascript is hard?", ["Yes", "No"], 0),
    new Question("PHP is hard?", ["Yes", "No"], 1),
    new Question("I program in?", ["Moring", "Afternoon", "Both"], 2)
  ];
  var index = Math.floor(Math.random() * quizz.length);
  var listIndexIncluded = [index];
  var questionIsResponsed = 0;
  var points = 0;
  callQuestion();

  function callQuestion() {
    questionIsResponsed += 1;
    quizz[index].displayQuestion();
    var answer = prompt("Please enter your answer", "");
    quizz[index].checkAnswer(answer);
    
  }

  function showResultat() {
    console.log("Fin quizz.\nVotre points:" + points);
  }

  function nextQuestion() {
    function getNextIndex() {
      var nextIndex = Math.floor(Math.random() * quizz.length);
      if (listIndexIncluded.includes(nextIndex)) return getNextIndex();
      else {
        return nextIndex;
      }
    }

    index = getNextIndex();
    listIndexIncluded.push(index);
    callQuestion();
  }
})();