
/**
 * @wraybowling spooty
 */

var tau = Math.PI*2

function triangle(t){
  return 1.0 - Math.abs(Math.round(t) - t) * 4
}

function sine(t){
  return Math.sin(Math.PI*2 * t);
}

function noise(t){
  return Math.random();
}

function envelope(t, measure, decay, release){
  var ts = t / 4 % measure;
  return Math.exp(-ts * decay * Math.exp(ts * release));
}

export function dsp(t) {
  
  var freq = 110;
  var detune = 10;
  var volume = 0.2;
  
  var mod = triangle(t*0.01);

  var osc1 = triangle(t*(freq - detune));
  var osc2 = sine(t*(freq + detune));
  
  var n = noise(t) * 0.3;
  
  return (osc1+osc2+n) * volume;
  
}