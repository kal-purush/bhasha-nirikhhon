
var intervalID;
function countdown() {
  intervalID = setInterval(textChange, 1000);
}
countdown();
var i = 4;
function textChange() {
  i--;
  var $countdownDisplay = document.querySelector('h1');
  if (i > 0) {
    $countdownDisplay.textContent = i;
  } else {
    $countdownDisplay.textContent = '~Earth Below US~';
    clearInterval(intervalID);
  }
}