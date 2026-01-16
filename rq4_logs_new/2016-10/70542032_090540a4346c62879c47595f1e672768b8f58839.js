// set countdown timer
var timeClock = 10;

// function to start the countdown timer
var startTimer = function() {

  // displays the time and replaces the stopwatch image
  var timer = setInterval(function() {
    var $newSpan = $('<span>').attr('class', 'timer');
    $('a').empty();
    $('a').append($newSpan).text(timeClock);
    timeClock -= 1;

    //clears the timer when it reaches 0
    if (timeClock === 0) {
      timeClock = 'Game Over';
      setTimeout(function() {
        clearInterval(timer);
      }, 1000);
    }
  }, 1000);
}

$('a').on('click', function(event) {

  startTimer();

});