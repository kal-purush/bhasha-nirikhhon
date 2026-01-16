var stopwatchInterval;
        var startTime;
        var lapCounter = 1;
        var pausedTime = 0;
        var isRunning = false;
        var isReset = false;

        function startStopwatch() {
            if (!isRunning) {
                startTime = Date.now() - (isReset ? 0 : pausedTime);
                stopwatchInterval = setInterval(updateTimer, 10);

                document.getElementById("startStopBtn").innerHTML = "Stop";
                document.getElementById("lapResetBtn").disabled = false;
                document.getElementById("lapResetBtn").innerHTML = "Lap";
                isRunning = true;
                isReset = false;
            } else {
                stopStopwatch();
            }
        }

        function updateTimer() {
            var currentTime = Date.now() - startTime;
            var minutes = Math.floor(currentTime / 60000);
            var seconds = Math.floor((currentTime % 60000) / 1000);
            var milliseconds = Math.floor((currentTime % 1000) / 10);

            minutes = (minutes < 10) ? "0" + minutes : minutes;
            seconds = (seconds < 10) ? "0" + seconds : seconds;
            milliseconds = (milliseconds < 10) ? "0" + milliseconds : milliseconds;

            document.getElementById("timer").innerHTML = minutes + ":" + seconds + ":" + milliseconds;
        }

        function lapResetAction() {
            if (isRunning) {
                recordLap();
            } else {
                resetStopwatch();
            }
        }

        function recordLap() {
            var lapTime = document.getElementById("timer").innerHTML;
            var lapElement = document.createElement("h3");
            lapElement.innerHTML = "Lap " + lapCounter + ": " + lapTime + "<hr>";

            document.getElementById("lap-container").appendChild(lapElement);
            lapCounter++;
        }

        function stopStopwatch() {
            clearInterval(stopwatchInterval);

            document.getElementById("startStopBtn").innerHTML = "Start";
            document.getElementById("lapResetBtn").innerHTML = "Reset";
            pausedTime = Date.now() - startTime;
            isRunning = false;
        }

        function resetStopwatch() {
            clearInterval(stopwatchInterval);
            document.getElementById("timer").innerHTML = "00:00:00";
            document.getElementById("lap-container").innerHTML = "";
            lapCounter = 1;
            pausedTime = 0;
            isReset = true;

            document.getElementById("startStopBtn").innerHTML = "Start";
            document.getElementById("lapResetBtn").disabled = true;
        }


//button Animation
var animateButton = function(e) {

    e.preventDefault;
    //reset animation
    e.target.classList.remove('animate');
    
    e.target.classList.add('animate');
    setTimeout(function(){
      e.target.classList.remove('animate');
    },700);
  };
  
  var bubblyButton = document.getElementsByClassName("bubbly-button");
  var bubblyButtons = document.getElementsByClassName("bubbly");

  for (var i = 0; i < bubblyButton.length; i++) {
    bubblyButton[i].addEventListener('click', animateButton, false);
  }
  for (var i = 0; i < bubblyButtons.length; i++) {
    bubblyButtons[i].addEventListener('click', animateButton, false);
  }

  