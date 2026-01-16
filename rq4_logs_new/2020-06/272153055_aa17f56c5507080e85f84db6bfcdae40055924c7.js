// Use class around DOM Elements
console.log("CONNECTED!");

const durationInput = document.querySelector('#duration');
const startButton = document.querySelector('#startButton');
const pauseButton = document.querySelector('#pauseButton');

const timer = new Timer(durationInput, startButton, pauseButton, {
  onStart(){
    console.log("Time to Start the Timer!");
  },
  onPause(){
    console.log("Timer has paused");
  },
  onComplete(){
    console.log("Times Up!");
  }
});