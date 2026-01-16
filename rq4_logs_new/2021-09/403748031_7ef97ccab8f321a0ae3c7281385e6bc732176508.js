const colors = [
    '#FFFFFF',
    '#2196F3',
    '#4CAF50',
    '#FF9800',
    '#009688',
    '#795548',
  ];
const refs = {
    body: document.querySelector("body"),
    start: document.querySelector(".btn-start"),
    stop: document.querySelector(".btn-stop"),
};
refs.stop.disabled = true;
const randomIntegerFromInterval = (min, max) => {
    return Math.floor(Math.random() * (max - min + 1) + min);
};
let colorIntervalId = null;
function changeBodyBackgroundColor (array) {
    refs.body.style.backgroundColor = colors[randomIntegerFromInterval(0,5)]
};
const onStartBtn = (e)=>{
    colorIntervalId = setInterval(()=>{
        changeBodyBackgroundColor(colors)
    }, 1000);
    refs.start.disabled = true;
    refs.stop.disabled = false;
};
const onStopBtn = (e)=>{
    clearInterval(colorIntervalId)
    refs.start.disabled = false;
};

refs.start.addEventListener('click', onStartBtn)
refs.stop.addEventListener('click', onStopBtn)
