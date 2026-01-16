let gamePaused = true;
const keyboard = new Keyboard();
const simpleTextChallange = new SimpleTextChallange();
simpleTextChallange.setUp();

function Keyboard() {
  this.previousKey = null;
  this.previousKeyPressed = null;
  this.timeOutKeypressedVar = null;

  this.highlight = function (element) {
    if (!gamePaused) {
      if (element !== null) {
        if (element == this.previousKey && this.previousKeyPressed) {
          clearTimeout(this.timeOut);
        } else {
          element.classList.add("pressed");
          this.previousKey = element;
          this.previousKeyPressed = true;
        }
        this.timeOut = setTimeout(() => {
          element.classList.remove("pressed");
          this.previousKeyPressed = false;
        }, 100);
      }
    }
  };
}

function SimpleTextChallange() {
  this.challengeStrings = [
    "Trying to make a wise, good choice when thinking about what kinds of careers might be best for you is a hard thing to do",
    "Your future may very well depend on the ways you go about finding the best job openings for you in the world of work",
    "All the bikers enjoyed the nice view when they came to the top. All the roads far below them looked like ribbons. A dozen or so boats could be seen on the lake. It was very quiet and peaceful and no one wished to leave.",
    "They had a burger at the lake and then rode farther up the mountain. As one rider started to get off his bike, he slipped and fell. One of the other bikers saw him fall but could do nothing to help him. Neither the boy nor the bike got hurt",
    "The bikers rode down the long and narrow path to reach the city park. When they reached a good spot to rest, they began to look for signs of spring",
    "The sun was bright, and a lot of bright red and blue blooms proved to all that warm spring days were the very best. Spring rides were planned",
    "Proper ergonomics at the workstation is a common topic considered. The Data Entry Clerk may also use a mouse, and a manually-fed scanner may be involved. Speed and accuracy, not necessarily in that order, are the key measures of the job; it is possible to do this job from home",
    "A data entry clerk is a member of staff employed to enter or update data into a computer system. Data is often entered into a computer from paper documents using a keyboard. The keyboards used can often have special keys and multiple colors to help in the task and speed up the work"
  ];

  this.currCharIndex = 0;
  this.challengeNode = document.querySelector(".challenge-string");
  this.challengeCharNode = this.challengeNode.children;
  this.intervalBlinkVar = null;
  this.stopwatch = new Stopwatch();
  this.previousResult = {
    duration: 0,
    speed: 0,
    errors: 0,
    score: 0,
  };
  this.total = {
    totalSpeed: 0,
    bestSpeed: 0,
    totalScore: 0,
  };
  this.challengeStringIndex = -1;

  this.enableBlink = () => {
    clearInterval(this.intervalBlinkVar);
    this.intervalBlinkVar = setInterval(() => {
      const currCharNode = this.challengeCharNode.item(this.currCharIndex);
      if (currCharNode.classList.contains("blink")) {
        currCharNode.classList.remove("blink");
      } else {
        currCharNode.classList.add("blink");
      }
    }, 500);
  };

  this.disableBlink = () => {
    this.challengeCharNode.item(this.currCharIndex).classList.remove("blink");
    clearInterval(this.intervalBlinkVar);
  };

  this.setUp = () => {
    let charNodes = "";
    this.challengeStringIndex =
      this.challengeStringIndex + 1 < this.challengeStrings.length
        ? this.challengeStringIndex + 1
        : 0;
    this.currCharIndex = 0;
    this.stopwatch.reset();

    this.challengeNode.innerHTML = "";
    for (
      let i = 0;
      i < this.challengeStrings[this.challengeStringIndex].length;
      i++
    ) {
      charNodes =
        charNodes +
        `<span class="challenge-char">${this.challengeStrings[
          this.challengeStringIndex
        ].charAt(i)}</span>`;
    }
    this.challengeNode.innerHTML = charNodes;
    if (!gamePaused) {
      this.enableBlink();
      this.stopwatch.start();
    }
  };

  this.play = (pressedKey) => {
    if (!gamePaused) {
      const key = pressedKey.key;

      const currCharNode = this.challengeCharNode.item(this.currCharIndex);
      if (key === currCharNode.textContent) {
        currCharNode.classList.add("disabled");
        currCharNode.classList.remove("blink");
        if (this.currCharIndex + 1 >= this.challengeNode.childElementCount) {
          // Challenge End
          this.disableBlink();

          // Calculate the score
          let duration = this.stopwatch.now(),
            speed = this.challengeNode.childElementCount / 5 / (duration / 60),
            errors = this.challengeNode.querySelectorAll(".incorrect").length,
            score = speed * 10 - errors / 2;
          let result = {
            duration: duration,
            speed: speed,
            errors: errors,
            score: score,
          };

          this.setUp();
          this.processResult(result);
        } else {
          this.currCharIndex++;
        }
      } else {
        currCharNode.classList.add("incorrect");
      }
    }
  };

  this.processResult = function (result) {
    const diff = {
      duration: result.duration - this.previousResult.duration,
      speed: result.speed - this.previousResult.speed,
      errors: result.errors - this.previousResult.errors,
      score: result.score - this.previousResult.score,
    };

    this.total.totalSpeed = this.total.totalSpeed + result.speed;
    this.total.bestSpeed =
      result.speed > this.previousResult.speed
        ? result.speed
        : this.previousResult.speed;
    this.total.totalScore = this.total.totalScore + result.score;

    const averageSpeed = this.total.totalSpeed / this.challengeStringIndex;
    const averageScore = this.total.totalScore / this.challengeStringIndex;

    document.getElementById("speed").innerHTML = Math.trunc(result.speed);
    if (diff.speed > 0) {
      document
        .getElementById("speed-diff-arrow")
        .classList.remove("fas", "fa-long-arrow-alt-down", "arrow-down");

      document
        .getElementById("speed-diff-arrow")
        .classList.add("fas", "fa-long-arrow-alt-up", "arrow-up");
    } else {
      document
        .getElementById("speed-diff-arrow")
        .classList.remove("fas", "fa-long-arrow-alt-up", "arrow-up");

      document
        .getElementById("speed-diff-arrow")
        .classList.add("fas", "fa-long-arrow-alt-down", "arrow-down");
    }
    document.getElementById("speed-diff").innerHTML = Math.trunc(diff.speed);

    document.getElementById("error").innerHTML = Math.trunc(result.errors);
    if (diff.errors <= 0) {
      document
        .getElementById("error-diff-arrow")
        .classList.remove("fas", "fa-long-arrow-alt-down", "arrow-down");
      document
        .getElementById("error-diff-arrow")
        .classList.add("fas", "fa-long-arrow-alt-up", "arrow-up");
    } else {
      document
        .getElementById("error-diff-arrow")
        .classList.remove("fas", "fa-long-arrow-alt-up", "arrow-up");

      document
        .getElementById("error-diff-arrow")
        .classList.add("fas", "fa-long-arrow-alt-down", "arrow-down");
    }
    document.getElementById("error-diff").innerHTML = Math.trunc(diff.errors);

    document.getElementById("score").innerHTML = Math.trunc(result.score);
    if (diff.score > 0) {
      document
        .getElementById("score-diff-arrow")
        .classList.remove("fas", "fa-long-arrow-alt-down", "arrow-down");

      document
        .getElementById("score-diff-arrow")
        .classList.add("fas", "fa-long-arrow-alt-up", "arrow-up");
    } else {
      document
        .getElementById("score-diff-arrow")
        .classList.remove("fas", "fa-long-arrow-alt-up", "arrow-up");

      document
        .getElementById("score-diff-arrow")
        .classList.add("fas", "fa-long-arrow-alt-down", "arrow-down");
    }
    document.getElementById("score-diff").innerHTML = Math.trunc(diff.score);

    document.getElementById("speed-avg").innerHTML =
      averageSpeed.toFixed(2) + "wpm";
    document.getElementById("speed-best").innerHTML =
      this.total.bestSpeed.toFixed(2) + "wpm";
    document.getElementById("score-avg").innerHTML = averageScore.toFixed(2);
    this.previousResult = result;
  };
}

function Stopwatch() {
  this.started = false;
  this.duration = 0;
  let intervalStopwatchVar = null;

  this.start = () => {
    if (!this.started) {
      intervalStopwatchVar = setInterval(() => {
        this.duration++;
      }, 1000);
      this.started = true;
    }
  };
  this.stop = () => {
    if (this.started) {
      clearInterval(intervalStopwatchVar);
      this.started = false;
    }
    return this.duration;
  };

  this.reset = () => {
    this.stop();
    this.duration = 0;
  };

  this.now = () => {
    return this.duration;
  };
}

if (sessionStorage.getItem("overlaySeen")) {
  document.getElementById("overlay-main").style.display = "none";
} else {
  document.getElementById("overlay-main").style.display = "block";
}

document.addEventListener("keydown", (e) => {
  const pressedKey = document.querySelector(`.keyboard .${e.code}`);
  keyboard.highlight(pressedKey);
});
document.addEventListener("keypress", simpleTextChallange.play);

document.getElementById("practice-type").addEventListener("click", (e) => {
  if (gamePaused) {
    document.getElementById("paused-overlay").style.display = "none";
    simpleTextChallange.stopwatch.start();
    simpleTextChallange.enableBlink();
    gamePaused = false;
  } else {
    document.getElementById("paused-overlay").style.display = "block";
    simpleTextChallange.stopwatch.stop();
    simpleTextChallange.disableBlink();
    gamePaused = true;
  }
});

function dissmissOverlay() {
  document.getElementById("overlay-main").style.display = "none";
  sessionStorage.setItem("overlaySeen", "true");
}