/*
  0 - verde
  1 - vermelho
  2 - amarelo
  3 - azul
*/

let order = [];
let clicked = [];
let score = 0;

const green = document.querySelector('.green');
const red = document.querySelector('.red');
const yellow = document.querySelector('.yellow');
const blue = document.querySelector('.blue');

const lightColor = (element, number) => {
  number *= 500;

  setTimeout(() => {
    element.classList.add('selected');
  }, number - 250);

  setTimeout(() => {
    element.classList.remove('selected');
  });
}

const shuffleOrder = () => {
  let colorOrder = Math.floor(Math.random() * 4);
  order[order.length] = colorOrder;
  clicked = [];

  for(let i in order) {
    let elementColor = createColorElement(order[i]);
    lightColor(elementColor, Number(i) + 1);
  }
}

const checkOrder = () => {
  for(let i in clicked) {
    if(clicked[i] !== order[i]) {
      gameOver();
      break;
    }
  }

  if(clicked.length === order.length) {
    alert(`Pontuação: ${score}\nVocê acertou! Iniciando o próximo nível.`);
    nextLevel();
  }
}

const click = (color) => {
  clicked[clicked.length] = color;
  createColorElement(color).classList.add('selected');

  setTimeout(() => {
    createColorElement(color).classList.remove('selected');
    checkOrder();
  }, 250);
}

const createColorElement = (color) => {
  if(color == 0) {
    return green;
  } else if(color == 1) {
    return red;
  } else if(color == 2) {
    return yellow;
  } else if(color == 3) {
    return blue;
  };
}

const nextLevel = () => {
  score += 1;
  shuffleOrder();
}

const playGame = () => {
  alert(`Bem vindo ao Genius! Iniciando o jogo.`);
  score = 0;

  nextLevel();
}

const gameOver = () => {
  alert(`Pontuação: ${score}\nVocê perdeu o jogo.\nClique em 'Ok' para iniciar um novo jogo.`);
  order = [];
  clicked = [];

  playGame();
}

// green.addEventListener('click', click(0));
// red.addEventListener('click', click(1));
// yellow.addEventListener('click', click(2));
// blue.addEventListener('click', click(3));

green.onclick = () => click(0);
red.onclick = () => click(1);
yellow.onclick = () => click(2);
blue.onclick = () => click(3);

playGame();