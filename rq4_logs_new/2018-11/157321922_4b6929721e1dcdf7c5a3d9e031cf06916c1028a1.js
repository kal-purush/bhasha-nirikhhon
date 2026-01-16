const WINDOW_SIZE = 500;
let gridSize = 16;
let divsArray = [];

const container = document.querySelector('.container');
const resetButton = document.querySelector('button');
resetButton.addEventListener('click', reset);
const radioButtons = document.querySelectorAll('input');

createGrid();

for (let i = 0; i < radioButtons.length; i++) {
  radioButtons[i].addEventListener('click', function() {
    gridSize = radioButtons[i].value;
    deleteGrid();
    createGrid();
  });
}

function createGrid() {
  let divSize = WINDOW_SIZE / (Math.sqrt(gridSize)) - 2;
  for (let i = 0; i < gridSize; i++) {
    divsArray[i] = document.createElement('div');
    divsArray[i].style.height = divSize + 'px';
    divsArray[i].style.width = divSize + 'px';
    divsArray[i].style.border = '1px solid black';
    divsArray[i].style.backgroundColor = 'rgb(255,255,255)';
    container.appendChild(divsArray[i]);
    divsArray[i].addEventListener('mouseover', colorIt);
  }
}

function deleteGrid() {
  while (container.firstChild) {
    container.removeChild(container.firstChild);
  }
}

function colorIt(e) {
  if (e.target.style.backgroundColor == 'rgb(255, 255, 255)') {
    e.target.style.backgroundColor = randomColor();
  } else {
    e.target.style.backgroundColor = shadeRGBColor(e.target.style.backgroundColor, -0.25);
  }
}

function reset() {
  for (var i = 0; i < gridSize; i++) {
    divsArray[i].style.backgroundColor = 'rgb(255,255,255)';
  }
}

function randomColor() {
  let R = Math.floor(Math.random() * (250 - 140)) + 140;
  let G = Math.floor(Math.random() * (250 - 140)) + 140;
  let B = Math.floor(Math.random() * (250 - 140)) + 140;
  return 'rgb(' + R + ',' + G + ',' + B + ')';
}

function shadeRGBColor(color, percent) {
  var f = color.split(","),
    t = percent < 0 ? 0 : 255,
    p = percent < 0 ? percent * -1 : percent,
    R = parseInt(f[0].slice(4)),
    G = parseInt(f[1]),
    B = parseInt(f[2]);
  return "rgb(" + (Math.round((t - R) * p) + R) + "," + (Math.round((t - G) * p) + G) + "," + (Math.round((t - B) * p) + B) + ")";
}