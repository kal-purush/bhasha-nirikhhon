var params = {
  break: {
    name: "Break",
    delay: 0.2,
    css: "break"
  },
  work: {
    name: "Work",
    delay: 0.2,
    css: "work"
  }
}
var steps = [params.work, params.break];


var currentStep = 0;
var endTime;
var paused = false;
var pauseTime = 0;
initTimer();
refresh();
pause();


setInterval(function() {
  refresh()
}, 100);

function initTimer() {
  endTime = new Date().getTime() + steps[currentStep].delay * 60 * 1000;
}

function refresh() {
  if (!paused) {
    var t = Math.ceil((endTime - new Date().getTime()) / 1000);
    var mins = Math.floor(t / 60);
    var secs = Math.floor(t - mins * 60);

    document.getElementById("timer").innerHTML = ("0" + mins).slice(-2) + ":" + ("0" + secs).slice(-2);

    if (t <= 0) {
      currentStep++;
      if (currentStep >= steps.length) {
        currentStep = 0;
      }
      var audio = new Audio('alarm.mp3');
      audio.play();
      initTimer();
      updateStepStyle();
    }
  }
}

function start() {
  if (paused) {
    endTime += new Date().getTime() - pauseTime;
    paused = false;
    updateButtonsStyle();
    updateStepStyle();
  }
}

function pause() {
  if (!paused) {
    paused = true;
    pauseTime = new Date().getTime();
    updateButtonsStyle();
  }
}

function updateButtonsStyle() {
  document.getElementById("start").disabled = !paused;
  document.getElementById("pause").disabled = paused;
}

function updateStepStyle() {
  document.getElementById("container").className = steps[currentStep].css;
  var stepText = document.getElementById("steptext")
  stepText.innerHTML = steps[currentStep].name;
  stepText.className = "animate";
  setTimeout(function() {
    stepText.className = "";
  }, 5000);
}
