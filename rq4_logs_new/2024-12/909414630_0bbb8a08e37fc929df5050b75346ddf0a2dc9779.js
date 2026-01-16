const sequence = [
  { key: "ArrowLeft", time: 1500 },
  { key: "ArrowRight", time: 2000 },
  { key: "ArrowLeft", time: 1000 },
];

let currentIndex = 0;
let startTime = null;
const resultsDiv = document.getElementById("results");
const sequenceDiv = document.getElementById("sequence");

function showSequence() {
  sequenceDiv.innerHTML = sequence
    .map((item, index) => 
      `<span ${index === currentIndex ? 'style="font-weight:bold;"' : ''}>
         ${item.key} (${item.time / 1000}s)
       </span>`
    ).join(" ➔ ");
}

function checkTiming(key, duration) {
  const target = sequence[currentIndex];
  if (key === target.key) {
    const accuracy = Math.abs(duration - target.time);
    resultsDiv.innerHTML += `<div class="result">Pressed ${key}: ${duration}ms (accuracy: ±${accuracy}ms)</div>`;
    currentIndex++;
    if (currentIndex < sequence.length) {
      startTime = performance.now();
    } else {
      resultsDiv.innerHTML += `<div class="result">Sequence Complete!</div>`;
    }
  } else {
    resultsDiv.innerHTML += `<div class="missed">Wrong key: ${key}</div>`;
  }
}

window.addEventListener("keydown", (event) => {
  if (currentIndex < sequence.length && startTime !== null) {
    const duration = performance.now() - startTime;
    checkTiming(event.key, duration);
    startTime = performance.now();
  }
});

function startGame() {
  resultsDiv.innerHTML = "";
  currentIndex = 0;
  startTime = performance.now();
  showSequence();
}

startGame();