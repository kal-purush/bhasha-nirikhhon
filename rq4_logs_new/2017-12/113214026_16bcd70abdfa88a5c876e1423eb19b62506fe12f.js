/***********************************************************************
* Name: Ryan Mack
* Date: 12/5/2017
* Description: A file that contains the code for a pomodoro clock
***********************************************************************/

// stores a reference to setInterval. Initialized in the function 'timer'
let countdown;

/***********************************************************************
* timer function -- sets and resets the a timer saved to the variable
* countdown.
***********************************************************************/
function timer(seconds) {
  // clear any existing timer
  clearInterval(countdown);

  // get the time at which the timer will stop
  const now = Date.now();
  const then = now + (seconds * 1000);
  displayTimeLeft(Math.round((then - Date.now()) / 1000));

  countdown = setInterval(() => {
    timeLeft = Math.round((then - Date.now()) / 1000);

    if (timeLeft <= 0) {
      clearInterval(countdown);
      return;
    }
    displayTimeLeft(timeLeft);
  }, 1000);
}

// takes in a parameter in seconds and formats it in a human friendly way
function displayTimeLeft(seconds) {
  const minutes = Math.floor(seconds / 60);
  seconds %= 60;
  console.log(padTime(minutes, seconds));
}

// pads time so it is consistent and readable. Instead of '5:3'
// (5 minutes and 3 seconds), it returns a string '05:03'
function padTime(minutes, seconds) {
  return `${minutes < 10 ? '0': ''}${minutes}:${seconds < 10 ? '0' : ''}${seconds}`
}


// timer(124);