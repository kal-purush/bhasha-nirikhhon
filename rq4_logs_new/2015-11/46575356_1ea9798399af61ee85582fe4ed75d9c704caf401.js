console.log("i iz loaded...")

// welcome player
alert("Welcome to the Parks and Recreation Trivia game!")

// display player name with prompt
var playerName = "Welcome, " + prompt("Woah, woah, woah! Where's the fire? Let's get to know each other. Tell me your name.") + "!"

// display player name in h2
function renderNameDisplay(){
  var $nameDisplay = $('#name-display');
  $nameDisplay.text(playerName);
}

renderNameDisplay();

// menu buttons


// display questions
$($displayQuestion = function(){
    $('.questions').hide();
    $('.questions:nth-child(1)').show();
    $('.next-button').on('click', function(next){
        // next.preventDefault();
        nextButton();
    });
});

// after clicking next button, show next question
var $qArray = $('.questions');
var q = 0;

function nextButton(){
  if (q < $qArray.length) {
    $qArray.hide();
    $($qArray[q]).show();
    q++;
  } else if (q = $qArray.length){
    $('.next-button').text('Submit Answers');
  }
  };

nextButton();

// if answer is correct, alert "correct"
// if answer is incorrect, alert "incorrect", try again?
//