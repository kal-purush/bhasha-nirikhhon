var time = new Date().getHours();
var oneSecond = 1000;

var noon = 12;
var evening = 18; // 6PM
var wakeUpTime = 7; // 7AM
var lunchTime = 12; // 12PM
var partyTime = 17; // 5PM
var isPartyTime = false; // default setting!
var napTime = lunchTime + 2; // 2PM

var clock = document.getElementById("clock");
var timeEvent = document.getElementById("timeEvent");
var lolcatImage = document.getElementById("lolcatImage");
var partyTimeButton = document.getElementById("partyTimeButton");
var wakeUpTimeSelector = document.getElementById("wakeUpTimeSelector");
var lunchTimeSelector = document.getElementById("lunchTimeSelector");
var napTimeSelector = document.getElementById("napTimeSelector");


// UPDATE THE CLOCK //
var updateClock = function() {
var messageText;
var image = "https://s3.amazonaws.com/media.skillcrush.com/skillcrush/wp-content/uploads/2016/08/normalTime.jpg";
if (time == partyTime) {
    image = "https://s3.amazonaws.com/media.skillcrush.com/skillcrush/wp-content/uploads/2016/09/cat4.jpg";
    messageText = "IZ PARTEE TIME!!";
} else if (time == napTime) {
    image = "https://s3.amazonaws.com/media.skillcrush.com/skillcrush/wp-content/uploads/2016/09/cat3.jpg";
    messageText = "IZ NAP TIME...";
} else if (time == lunchTime) {
    image = "https://s3.amazonaws.com/media.skillcrush.com/skillcrush/wp-content/uploads/2016/09/cat2.jpg";
    messageText = "IZ NOM NOM NOM TIME!!";
} else if (time == wakeUpTime) {
    image = "https://s3.amazonaws.com/media.skillcrush.com/skillcrush/wp-content/uploads/2016/09/cat1.jpg";
    messageText = "IZ TIME TO GETTUP.";
} else if (time < noon) {
    messageText = "Good Morning!";
} else if (time > evening) {
    messageText = "Good Evening!";
} else {
    messageText = "Good Afternoon!";
}
timeEvent.innerText = messageText;
lolcatImage.src = image;
showCurrentTime();
};

// DISPLAY THE CLOCK STRING //
var showCurrentTime = function() {
  var currentTime = new Date();
  var hours = currentTime.getHours();
  var minutes = currentTime.getMinutes();
  var seconds = currentTime.getSeconds();
  var meridian = "AM";

  // Set Hours
  if (hours >= noon) {
    meridian = "PM"; 
  }
  if (hours > noon) { 
    hours = hours - 12;
  }

  // Set Minutes
  if (minutes < 10) {
    minutes = "0" + minutes;
  }

  // Set Seconds
  if (seconds < 10) {
    seconds = "0" + seconds;
  }
 
  // concantenate the time
  var clockTime = hours + ":" + minutes + ":" + seconds + " " + meridian;
  clock.innerText = clockTime;
};
updateClock();
setInterval(updateClock, oneSecond);

// PARTY TIME TOGGLE BUTTON //
var partyEvent = function() {
   if (isPartyTime === false) { 
        isPartyTime = true; 
        time = partyTime;
        partyTimeButton.innerText = "Party Over";
        partyTimeButton.style.background = "#0A8DAB";
    } else { 
        isPartyTime = false;
        time = new Date().getHours();
        partyTimeButton.innerText = "Party Time!";
        partyTimeButton.style.background = "#222";
    }
};

// MANAGE TIMED EVENT SELECTORS //
var wakeUpEvent = function() {
  wakeUpTime = wakeUpTimeSelector.value;
  console.log("wake up time changed to " + wakeUpTime);
};
var lunchEvent = function() {
  lunchTime = lunchTimeSelector.value;
  console.log("lunch time changed to " + lunchTime);
};
var napEvent = function() {
   napTime = napTimeSelector.value;
  console.log("nap time changed to " + napTime);
};

// LISTENERS //
partyTimeButton.addEventListener("click", partyEvent);
wakeUpTimeSelector.addEventListener("change", wakeUpEvent);
lunchTimeSelector.addEventListener("change", lunchEvent);
napTimeSelector.addEventListener("change", napEvent);