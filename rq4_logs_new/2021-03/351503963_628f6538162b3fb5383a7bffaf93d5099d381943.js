'use strict';
const MINE = '💣';
const FLAG = '🚩';
const EMPTY = '';
const HELP = `👀`



var gFirstClick = true;
var gLevel = { SIZE: 4, MINES: 2, left: '40%', width: '120px' };
var gBoomSound = new Audio('../sounds/boom.mp3')
var gLostSound = new Audio('../sounds/lost.wav')
var gWonSound = new Audio('../sounds/won.wav')

var gSafeClick = 3;
// var glastClick = [];
var gLife = 0;
var countHinit = 0;

var gBoard;

var gHinit = false;
var gElHinit;

var gGame = {
  isOn: false,
  mines: gLevel.MINES,
  shownCount: 0,
  markedCount: 0,
  secsPassed: 0
};

function init() {
  stopTimer();
  gBoard = buildBoard();
  initRenderGame();
  renderBoard();
  document.querySelector('.time').style.display = 'none';
  changeUI('CLICK CELL TO START GAME', '🚧', 'var(--bg1)', 10000);
  gGame.isOn = true;
  gGame.mines = gLevel.MINES;
  gGame.shownCount = 0;
  gGame.markedCount = 0;
}

function buildBoard() {
  var board = [];
  for (var i = 0; i < gLevel.SIZE; i++) {
    board[i] = [];
    for (var j = 0; j < gLevel.SIZE; j++) {
      //build board
      let cellData = {
        minesAroundCount: 0,
        isHint: false,
        isShown: false,
        isMine: false,
        isMarked: false,
      }
      board[i][j] = cellData;
    }
  }

  gLife = countHinit = gLevel.SIZE / 4;
  gFirstClick = true;
  return board;
}

localStorage.setItem(`record${(gLevel.SIZE / 4)}`, `${gGame.secsPassed}`);


function setLevel(event) {
  document.querySelector('.dropLevelbtn').textContent = event.textContent;
  switch (event.textContent) {
    case 'Beginner':
      gLevel = { SIZE: 4, MINES: 2, left: '40%', width: '120px' };
      break;
    case 'Medium':
      gLevel = { SIZE: 8, MINES: 12, left: '30%', width: '270px' };
      break;
    case 'Expert':
      gLevel = { SIZE: 12, MINES: 30, left: '25%', width: '330px' };
      break;
    default:
      break;
  }
  document.querySelector('.board-container').style.top = '34px';
  changeUI(`LEVEL :${event.textContent}  !\t--GOOD LUCK!--`, '🚀', '#B33771', 2000);
  setTimeout(() => {
    init()
  }, 2000);
}

function startGame(locationCell) {
  //להשים פצצות
  //להפעיל טיימר
  document.querySelector('.time').style.display = 'block';
  placedMines(locationCell);
  setMinesNegsCount();
  changeUI('START GAME!!!', '🏁', '#34ace0', 800)
  startTimer();
  renderBoard(gBoard, '.board-container');
  gFirstClick = false;
}

function placedMines(locationCell) {
  for (var index = 0; index < gLevel.MINES; index++) {
    var i = getRandomIntInclusive(0, gLevel.SIZE - 1);
    var j = getRandomIntInclusive(0, gLevel.SIZE - 1);
    if (((locationCell.i !== i) || (locationCell.j !== j)) && (gBoard[i][j].isMine === false)) {
      gBoard[i][j].isMine = true;
      gBoard[i][j].minesAroundCount = null;
    } else {
      index--;
    }
  }
}

function setMinesNegsCount() {
  for (var i = 0; i < gLevel.SIZE; i++) {
    for (var j = 0; j < gLevel.SIZE; j++) {
      if (!gBoard[i][j].isMine) gBoard[i][j].minesAroundCount = countNeighbors(i, j);
    }
  }
}


function chooseAction(event) {
  if (event.srcElement.localName === 'i') {
    var validHinit = event.target.getAttribute('data-valid');
    if (validHinit === 'true') {
      gHinit = true;
      gElHinit = event.target.id;
    }
  } else {
    var elCell = event.target;
    var locationCell = extractLocation(elCell.className.slice(9));
    if (gFirstClick) startGame(locationCell);
    if (event.button === 0 && checkLeftClick(locationCell)) {
      // glastClick.push('clicked', locationCell.i, locationCell.j)
      cellClicked(locationCell.i, locationCell.j)
    } else if (event.button === 2 && checkRightClick(locationCell)) {
      // glastClick.push('marked', locationCell.i, locationCell.j)
      cellMarked(locationCell.i, locationCell.j);
    } else {
      return null
    }
    checkVictory();
    renderBoard(gBoard, '.board-container');
  }
}

function checkLeftClick(locationCell) {
  if (!gBoard[locationCell.i][locationCell.j].isShown && !gBoard[locationCell.i][locationCell.j].isMarked) return true;
  return false;
}

function checkRightClick(locationCell) {
  if ((gBoard[locationCell.i][locationCell.j].isShown) ||
    ((gGame.markedCount === gGame.mines) &&
      (!gBoard[locationCell.i][locationCell.j].isMarked))
  ) {
    return false;
  } else {
    return true;
  }
}

function cellClicked(i, j) {
  if (gHinit) {
    useHiting(i, j);
  } else {
    if (gBoard[i][j].isMine === true) {
      gBoard[i][j].isShown = true;
      document.querySelector('.smile').textContent = '😦';
      gBoomSound.play();
      checkGameOver();
    } else if (gBoard[i][j].minesAroundCount > 0) {
      document.querySelector('.smile').textContent = '😀';
      gBoard[i][j].isShown = true;
      gGame.shownCount++;

    } else {
      expandShownRec(i, j);
      setTimeout(() => {
        changeUI(`AMAIZING - LOCATION!!`, '🎯', '#218c74', 2000);
      }, 500);
    }
    renderBoard();
  }
}

function cellMarked(i, j) {
  gBoard[i][j].isMarked = !(gBoard[i][j].isMarked);
  gBoard[i][j].isMarked ? gGame.markedCount++ : gGame.markedCount--;
  gBoard[i][j].isMarked ? renderCell({ i: i, j: j }, FLAG) : renderCell({ i: i, j: j }, EMPTY)
  changeUI(`BE CARFULL!!!`, '📢', '#ff793f', 2000);
}

function expandShownRec(i, j) {
  if (!gBoard[i][j].isShown && !gBoard[i][j].isMarked) {
    gBoard[i][j].isShown = true;
    gGame.shownCount++
    renderBoard();
    var neighbors = checkExpose(i, j);
    for (let index = 0; index < neighbors.length; index++) {
      if (gBoard[neighbors[index].i][neighbors[index].j].minesAroundCount > 0) {
        if (!gBoard[neighbors[index].i][neighbors[index].j].isShown) gGame.shownCount++
        gBoard[neighbors[index].i][neighbors[index].j].isShown = true;
        neighbors[index].show = true;
      }
      else {
        expandShownRec(neighbors[index].i, neighbors[index].j);
      }
    }
  }
}

function useHiting(i, j) {
  changeUI("DONT WORRY HINIT COMING!!! ", HELP, '#ffb142', 3000)
  var hinitNeg = checkExpose(i, j, gBoard, true);
  hinitNeg.push({ i: i, j: j })
  hinitNeg.forEach(el => gBoard[el.i][el.j].isHint = true);
  renderBoard();
  setTimeout(() => {
    hinitNeg.forEach(el => gBoard[el.i][el.j].isHint = false);
    renderBoard();
    document.querySelector(`#${gElHinit}`).style.display = 'none'
    gHinit = false;
  }, 1000);
}


function checkGameOver() {
  var life = document.querySelector(".life");
  if (life.hasChildNodes()) {
    life.removeChild(life.childNodes[0]);
    gLife--;
    if (gLife === 0) {
      changeUI(`BOOOM!! LEFT ${gLife} life`, MINE, 500);
      gameOver()
    } else {
      changeUI(`BOOOM!! LEFT ${gLife} life`, MINE, '#EA2027', 2500);
    }
  }
}

function gameOver() {
  stopTimer();
  gLostSound.play();
  document.querySelector('.smile').textContent = '💀';
  changeUI('GAME OVER!!! PRESS 😀 AND TRY AGAIN!!!', '👹', '#b33939', 10000);
  gGame.isOn = false;
}

function checkVictory() {
  if ((gGame.markedCount === gGame.mines) && (gGame.shownCount === (gLevel.SIZE * gLevel.SIZE - gLevel.MINES))) {
    gWonSound.play();
    gGame.isOn = false;
    checkRecord();
    stopTimer();
    changeUI('WINNERRRR!!! PRESS 😀 AND TRY AGAIN!!!', '🏆', '#fff', 2000);
  }
}

function checkRecord() {
  gGame.secsPassed = parseInt(document.querySelector('.time').textContent);
  var lastRecord = parseInt(localStorage.getItem(`record${(gLevel.SIZE / 4)}`));
  if ((lastRecord === NaN) || (gGame.secsPassed < lastRecord)) {
    localStorage.setItem(`record${(gLevel.SIZE / 4)}`, `${gGame.secsPassed}`);
    //show modal of record
  }
}

function safeClick() {
  if (gSafeClick > 0) {
    var emptyCells = getEmptyCells();
    if (emptyCells.length > 0) {
      gSafeClick--;
      if(gSafeClick === 0 ) document.querySelector('.fa-share-square').style.display = `none`;
      var el = emptyCells[getRandomIntInclusive(0, emptyCells.length)];
      gBoard[el.i][el.j].isShown = true;
      changeUI("SAFE CLCIK!!! WATCH QUICLY ", HELP, '#ffb142')
      renderBoard();
      setTimeout(() => {
        gBoard[el.i][el.j].isShown = false;
        renderBoard();
      }, 2000);
      document.querySelector('.fa-share-square').textContent = ` ${gSafeClick}`;
    }else{
      gSafeClick++;
      changeUI("CANNOT START SAFE CLCIK!!!", '⚡', '#a29bfe')
    }
  }
}

function saveLastClick(action, i, j){
  glastClick = {action: action, i:i, j: j};
}

// function undoClick() {
//   var lastClick = glastClick.pop();
//   if(action == 'clicked'){
//     if(gBoard[i][j].isMine === true){
//       //תכסה ותוסיף חיים
//     }else{

//     }
//   }else()
// }