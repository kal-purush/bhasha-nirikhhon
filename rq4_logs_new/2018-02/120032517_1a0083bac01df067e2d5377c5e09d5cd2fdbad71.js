var osc, mod, slider_amp;
var env;

//assignated values to all elements of the envelope, so they can be changed easily
var attackLevel = 1.0;
var releaseLevel = 0; 

var attackTime = 0.001;
var decayTime = 0.3;
var susPercent = 0.4;
var releaseTime = 0.5;

function setup() {
  createCanvas(400, 400);
  background(200); 

  text("AMPLITUDE", 50, 225);

  slider_amp = createSlider (20,1200,0);
  slider_amp.position(50,250);
  slider_amp.mousePressed(sldramp);
  
  //CARRIER, oscillator that will be heard
  osc = new p5.Oscillator();
  osc.amp(0);
  osc.freq(440);    
  
  
  
  
  env = new p5.Env(); 
  env.setADSR(attackTime, decayTime, susPercent, releaseTime);
  env.setRange(attackLevel, releaseLevel);

} 

function keyPressed(){ 
  osc.start(); //start oscillating until a valid key is pressed
  switch (key) {
  case "Q":
    console.log("Q was pressed");
    osc.freq(261.6255); //specific note frequency
    break;
   case "W":
    console.log("W was pressed");
    osc.freq(277.1826);
    break;
   case "E":
    console.log("E was pressed");
    osc.freq(293.6647);
    break;
   case "R":
    console.log("R was pressed");
    osc.freq(311.1269);
    break;
   case "T":
    console.log("T was pressed");
    osc.freq(349.2282);
    break;
   case "Y":
    console.log("Y was pressed");
    osc.freq(369.9944);
    break;
   case "U":
    console.log("U was pressed");
    osc.freq(391.9954);
    break;
   case "I":
    console.log("I was pressed");
    osc.freq(415.3046);
    break;
   case "O":
    console.log("O was pressed");
    osc.freq(440);
    break;
   case "P":
    console.log("P was pressed");
    osc.freq(466.1637);
    break;
   case "A":
    console.log("A was pressed");
    osc.freq(493.8833);
    break; 
    default:
    console.log(key + " IS INVALID");
      osc.stop(); //whenever another key is pressed, the oscillator stops, so it doesn't sound the same as the last key pressed
    break;
  
  }
    env.triggerAttack(osc); //carrier connected to the envelope
}
 
  
function keyReleased(){ 
  console.log("r");
  env.triggerRelease();
}

function draw(){ 
  //to change the values of frequency and amplitude in real-time
  osc.amp(slider_amp.value());
}


function sldramp(){ 
}