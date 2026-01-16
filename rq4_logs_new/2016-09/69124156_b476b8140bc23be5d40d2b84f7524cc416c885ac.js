document.addEventListener("DOMCOntentLoaded",
  function (event) {
    function answer (event) {
      var botAnswer = Math.random();

      if (botAnswer <= 0.3333) {
        botAnswer = "rock";
      } else if (botAnswer <= 0.6666) {
        botAnswer = "paper";
      } else {
        botAnswer = "scissors";
      }

      function compare() {
        
      }
    } //function userAnswer 

    document.querySelector("button")
    .addEventListener("click", answer)
  } //function
);