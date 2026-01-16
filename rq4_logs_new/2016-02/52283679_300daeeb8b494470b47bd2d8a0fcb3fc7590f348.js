// send an object containing the current time's hours:minutes:seconds to setTime
function getTime() {
  var date = new Date(),
      hours = date.getHours().toString(),
      minutes = date.getMinutes().toString(),
      seconds = date.getSeconds().toString();
  if(hours.length < 2) {
    hours = '0'+hours;
  }
  if(minutes.length < 2) {
    minutes = '0'+minutes;
  }
  if(seconds.length < 2) {
    seconds = '0'+seconds;
  }
  setTime({
    hours: hours,
    minutes: minutes,
    seconds: seconds
  });
}

// cache references to DOM elements, then use the object from getTime() to fill their contents
var elements = {
  hours: document.getElementById('hours'),
  minutes: document.getElementById('minutes'),
  seconds: document.getElementById('seconds'),
  color: document.getElementById('color')
}

function setTime(date) {
    elements.hours.innerText = date.hours;
    elements.minutes.innerText = date.minutes;
    elements.seconds.innerText = date.seconds;
    elements.color.innerText = date.hours +''+date.minutes+''+date.seconds;
    document.body.style.backgroundColor = '#'+elements.color.innerText;
    setTimeout(getTime, 1000);
}


(function init() {
  getTime();
})();