const colors = [
  '#FFFFFF',
  '#2196F3',
  '#4CAF50',
  '#FF9800',
  '#009688',
  '#795548',
];

const refs = {
  startBtn: document.querySelector('button[data-action="start"]'),
  stopBtn: document.querySelector('button[data-action="stop"]'),
  bodyEl: document.querySelector('body'),
};

const DELAY = 1000;
let intervalId = null;
let isActive = false;

refs.startBtn.addEventListener('click', onOpenBtnClick);
refs.stopBtn.addEventListener('click', onCloseBtnClick);

// Натискання кнопки
function onOpenBtnClick() {
  if (isActive) {
    return;
  }

  intervalId = setInterval(onChangeColor, DELAY);
  isActive = true;
}

// Зупинка
function onCloseBtnClick() {
  isActive = false;
  clearInterval(intervalId);
}

// Зміна кольору бекграунду
function onChangeColor() {
  let color = randomIntegerFromInterval(0, colors.length - 1);
  refs.bodyEl.style.backgroundColor = colors[color];
}

const randomIntegerFromInterval = (min, max) => {
  return Math.floor(Math.random() * (max - min + 1) + min);
};