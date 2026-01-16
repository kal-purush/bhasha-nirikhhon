//global constants
const cluePauseTime = 333; //how long to pause in between clues
const nextClueWaitTime = 1100; //how long to wait before starting playback of the sequence

//Global Variables
var pattern = [3, 4, 2, 1, 2, 3, 4, 2, 1, 2, 3, 4, 6, 5, 4, 7, 3, 2];
var progress = 0;
var gamePlaying = false;
var tonePlaying = false;
var volume = 0.5; //must be between 0.0 and 1.0
var guessCounter = 0;
var mistakes = 0;
var clueHoldTime = 1000; //how long to hold each clue's light/sound
var gameNum = 0;

function patternGen(){
  pattern=[];
  for(let i=0; i<8;i++){
    pattern.push(Math.floor(Math.random()*7)+1); //[1]
  }
}

function startGame(){
  //initialize game varibales
  progress = 0;
  gamePlaying = true;
  clueHoldTime = 1100; 
  mistakes = 0;
  if(gameNum>0){
    patternGen();
  }
  // swap the Start and Stop buttons
  document.getElementById("startBtn").classList.add("hidden");
  document.getElementById("stopBtn").classList.remove("hidden");
  playClueSequence();
  gameNum++;
}

function stopGame(){
  gamePlaying = false;
  document.getElementById("startBtn").classList.remove("hidden");
  document.getElementById("stopBtn").classList.add("hidden");
}

// Sound Synthesis Functions
const freqMap = {
  1: 452.04,
  2: 500.29,
  3: 558.84,
  4: 659.85,
  5: 779.85,
  6: 879.85,
  7: 599.85
}
function playTone(btn,len){ 
  o.frequency.value = freqMap[btn]
  g.gain.setTargetAtTime(volume,context.currentTime + 0.05,0.025)
  tonePlaying = true
  setTimeout(function(){
    stopTone()
  },len)
}
function startTone(btn){
  if(!tonePlaying){
    o.frequency.value = freqMap[btn]
    g.gain.setTargetAtTime(volume,context.currentTime + 0.05,0.025)
    tonePlaying = true
  }
}
function stopTone(){
    g.gain.setTargetAtTime(0,context.currentTime + 0.05,0.025)
    tonePlaying = false
}

//Page Initialization
// Init Sound Synthesizer
var context = new AudioContext()
var o = context.createOscillator()
var g = context.createGain()
g.connect(context.destination)
g.gain.setValueAtTime(0,context.currentTime)
o.connect(g)
o.start(0)

function lightButton(btn){
  document.getElementById("button"+btn).classList.add("lit");
  document.getElementById("img"+btn).classList.remove("hidden");
}

function clearButton(btn){
  document.getElementById("button"+btn).classList.remove("lit");
  document.getElementById("img"+btn).classList.add("hidden");
}

function playSingleClue(btn){
  if(gamePlaying){
    lightButton(btn);
    playTone(btn,clueHoldTime);
    setTimeout(clearButton,clueHoldTime,btn);
  }
}

function playClueSequence(){
  guessCounter = 0;
  let delay = nextClueWaitTime; //set delay to initial wait time
  for(let i=0;i<=progress;i++){ //for each clue that is revealed so far
    console.log("play single clue: " + pattern[i] + " in " + delay + "ms")
    setTimeout(playSingleClue,delay,pattern[i]) //set a timeout to play that clue
    delay += clueHoldTime 
    delay += cluePauseTime;
  }
  clueHoldTime-=50;
}

function loseGame(){
  stopGame();
  alert("Game Over. You lost.");
}

function winGame(){
  stopGame();
  alert("Game Over. You won!");
}


function guess(btn){
  console.log("user guessed: " + btn);
  if(!gamePlaying){
    return;
  }

  if (btn == pattern[guessCounter]){
    if (progress == guessCounter){
      if(progress == pattern.length - 1){
        winGame();
      }
      else {
        progress+=1;
        playClueSequence();
      }
    }
    else{
      guessCounter+=1;
    }
  }
  else if(mistakes<2){
    mistakes++;
    playClueSequence();
  }
  else{
    loseGame();
  }
}