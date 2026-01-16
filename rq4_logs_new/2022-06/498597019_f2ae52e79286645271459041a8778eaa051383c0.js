// Write a function called countdown that accepts a number as a parameter and every 1000 milliseconds decrements the value and console.logs it. Once the value is 0 it should log “DONE!” and stop.

const countdown = function(int) {
  // check that argument is a number, greater than or equal to 0, and divisible by 1
  try {
    if (typeof int !== "number") throw new Error();
    if (int < 0) throw new Error();
    if (int % 1 !== 0) throw new Error();
  } catch(e) {
    console.log(`Invalid argument: ${int}. countdown takes one argument which must be a whole number greater than or equal to 0`);
    return;
  }
  
  if (int === 0) {
    console.log("DONE!");
    return;
  }
  
  // proceed
  let intervalId = setInterval(function() {
    int--;
    if (int <= 0) {
      console.log("DONE!");
      clearInterval(intervalId);
    } else console.log(int);
    
  }, 1000)
}

// Tests
// countdown("banana");
// countdown(-3);
// countdown(1.8);
// countdown(7);
// countdown(1);
// countdown(0);



// Write a function called randomGame that selects a random number between 0 and 1 every 1000 milliseconds and each time that a random number is picked, add 1 to a counter. If the number is greater than .75, stop the timer and console.log the number of tries it took before we found a number greater than .75.

const randomGame = function() {
  
  let counter = 0;

  let intervalId = setInterval(function() {
    let random = Math.random();
    counter++;

    // printing each iteration for testing
    // let prettifiedRandom = Math.round(random*1000) / 1000;
    // console.log(`random: ${prettifiedRandom}, counter: ${counter}`)

    if (random > 0.75) {
      console.log(`It took ${counter} tries to get above 0.75`);
      clearInterval(intervalId);
    } 
  }, 1000);

}

randomGame();