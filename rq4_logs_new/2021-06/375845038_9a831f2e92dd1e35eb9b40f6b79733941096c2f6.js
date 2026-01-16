const queryString = window.location.search;
const urlParams = new URLSearchParams(queryString);
const blinksterID = urlParams.get('ref');
var discount = urlParams.get('discount');

if(discount === null) {
  discount = "VIP-8675309";
}

function buyBlinksAction() {
  window.location.href='https://move38.com/discount/' + discount + '?redirect=/cart/37584214130865:1&ref='+blinksterID;
};

var _statcounter = _statcounter || []; 
_statcounter.push({"tags": {"blinksterID": blinksterID, "discount": discount, "page": "FnF Deal"}}); 

window.onscroll = function() {scrollRotate()};

function scrollRotate() {
	// rotate each of the games based on the position of the page... stop rotating at specific points
  var windowOffset = window.pageYOffset;
  //console.log("window offset: " + windowOffset);
  
  var tiltAngle = 15;
  var tiltStart = 200;
  var tiltOffset = 275;
  var tiltRange = 300;  
  
  // Rotate hexagons:
  var hex1 = document.getElementById("hex-small");
  hex1.style.transform = "rotate("+ getRotation(0,600,30) + "deg)";
  
  var hex2 = document.getElementById("hex-medium");
  hex2.style.transform = "rotate("+ getRotation(0,600,-30) + "deg)";
  
  var hex3 = document.getElementById("hex-large");
  hex3.style.transform = "rotate("+ getRotation(0,600,-15) + "deg)";
  
  /* Function to compute how much to rotate
   *
   *  start = a starting point in pixels
   *  stop = stopping point in pixels
   *  amount = degree rotation
   */ 
  function getRotation(start, stop, amount) {
    var gameRotate;
    if(windowOffset < start) {
      gameRotate = 0;
    }
    else if(windowOffset >= start && windowOffset < stop) {
      gameRotate = (windowOffset-start)/((stop-start)/amount);
    }
    else {
      gameRotate = (stop-start)/((stop-start)/amount);
    }
  
    return gameRotate;
  }


}