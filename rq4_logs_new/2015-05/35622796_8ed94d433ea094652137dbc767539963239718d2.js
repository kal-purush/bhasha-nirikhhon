var e = new Date();
var x = e.getTime();
// Add analogue hand with Date() obj times by whatever, put that in the Vector2 for position
// 144/60 = 2.4 (for the minute hand)
// Import required modules
var UI = require('ui');
var Vector2 = require('vector2');

console.log('Ready!');

// Create the window
var mainFace = new UI.Window({ fullscreen: true });

// This is the part you see as the time. Code fairly self-explanatory
var theTime = new UI.TimeText({
  position: new Vector2(1,14),
  size: new Vector2(143,140),
  font: 'bitham-42-light',
  textAlign: 'center',
  text: '%H\n%M\n%S',
  color: 'black'
});

// This is the circle background
var circ = new UI.Circle({
  position: new Vector2(72,84),
  radius: 64,
  backgroundColor: 'white'
});

var minBar = new UI.Rect({
  position: new Vector2(0,0),
  size: new Vector2(144,12),
  backgroundColor: 'white',
});

var hourBar = new UI.Rect({
  position: new Vector2(0,156),
  size: new Vector2(144,12),
  backgroundColor: 'white'
});

// Add the elements to the mainFace window, with specific ordering
mainFace.add(circ);
mainFace.add(theTime);
mainFace.add(minBar);
mainFace.add(hourBar);

// Display window to user
mainFace.show();

setInterval(updateMins,1000);
setInterval(updateHours,1000);

// Minute hand
function updateMins() {
  var d = new Date();
  var mins = d.getMinutes();
  var pos1 = Math.round(mins*2.4);

  var minHand = new UI.Rect({
    position: new Vector2(pos1,0),
    size: new Vector2(2,12),
    color: 'black',
    backgroundColor: 'black'
  });
  setInterval(removeMins,1000);
  function removeMins() {
    mainFace.remove(minHand);
  }
  mainFace.add(minHand);
}
function updateHours() {
  var d = new Date();
  var hours = d.getHours();
  var pos2 = Math.round(hours*6);

  var hourHand = new UI.Rect({
    position: new Vector2(pos2,156),
    size: new Vector2(2,12),
    color: 'black',
    backgroundColor: 'black'
  });
  setInterval(removeHours,1000);
  function removeHours() {
    mainFace.remove(hourHand);
  }
  mainFace.add(hourHand);
}
var f = new Date();
var y = f.getTime();
var z1 = x-y;
var z = z1-z1-z1;
console.log('Fully loaded in ' + z + ' milliseconds.');