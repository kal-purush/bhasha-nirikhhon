class MineGrid {
  
  constructor(rowSize, mineRatio) {
    this.rowSize = rowSize;
    this.gridSize = rowSize * rowSize;
    this.mineRatio = mineRatio;
    this.numMines = Math.floor(this.gridSize * this.mineRatio);
    this.reveals = {};
    this.mines = this.genMines();
  }

  

  adjacents(guess) {
    let gx = guess[0];
    let gy = guess[1];

    let adjacent = [
      [gx + 1, gy],
      [gx + 1, gy + 1],
      [gx, gy + 1],
      [gx - 1, gy + 1],
      [gx - 1, gy],
      [gx - 1, gy - 1],
      [gx, gy - 1],
      [gx + 1, gy - 1]
    ];

    return adjacent.filter(val => {
      return (
        val[0] >= 0 &&
        val[0] < this.rowSize &&
        val[1] >= 0 &&
        val[1] < this.rowSize
      );
    });
  }

  genMines() {
    var mines = new Set();

    while (mines.size < this.numMines) {
      mines.add(Math.floor(Math.random() * this.gridSize));
    }

    return mines;
  }

  printReveals() {
    var line = "";

    for (var i = 0; i < this.gridSize; i++) {
      if (i in this.reveals) {
        line = line + this.reveals[i];
      } else if (this.mines.has(i)) {
        line = line + "m";
      } else {
        line = line + "?";
      }
      if ((i + 1) % this.rowSize == 0) {
        console.log('line is ' + line);
        line = "";
      }
    }
  }

  mineXY(pos) {
    let y = Math.floor(pos / this.rowSize);
    let x = pos % this.rowSize;
    return [y, x];
  }

  minePos(yx) {
    return yx[0] * this.rowSize + yx[1];
  }

  numAdjacentMines(guess) {
    var numAdj = 0;
    var adj = this.adjacents(guess);
    for (var i = 0; i < adj.length; i++) {
      if (this.mines.has(this.minePos(adj[i]))) {
        numAdj++;
      }
    }
    return numAdj;
  }

  revealMine(guess) {
    if (guess in this.reveals) {
      return;
    }
    let na = this.numAdjacentMines(this.mineXY(guess));
    this.reveals[guess] = na;
    if (na == 0) {
      let adj = this.adjacents(this.mineXY(guess));
      for (var i = 0; i < adj.length; i++) {
        this.revealMine(this.minePos(adj[i]));
      }
    }
  }
}

var lostGame = false;
var lostGuess = 0;

renderSquares(64);

function renderSquares(numsquares) {
  for (var i = 0; i < numsquares; i++) {
    var idiv = document.createElement("div");
    idiv.className = "square";
    idiv.internalId = i;
    idiv.innerText = "?";
    idiv.onclick = function() {
      handleClick(this.internalId);
    };
    document.getElementById("container").appendChild(idiv);
  }
}
// if (
//   Object.keys(this.reveals).length >=
//   this.gridSize - this.mines.length
// ) {
//   console.log("you win no more mines");
//   return;
function handleClick(id) {
  console.log(id);
  console.log(mg.mines);

  if (mg.mines.has(id)) {
    lostGame = true;
    lostGuess = id;
  } else {
    mg.revealMine(id);
  }
  renderReveals();
}

var mg = new MineGrid(8, 0.2);

function renderReveals() {
  const container = document.getElementById("container");
  for (var i = 0; i < mg.gridSize; i++) {
    if (i in mg.reveals) {
      container.children[i].innerHTML =
        mg.reveals[i] == 0 ? '<i class="fas fa-expand"></i>' : mg.reveals[i];
    } else if (mg.mines.has(i)) {
      if (lostGuess > -1 && i === lostGuess) {
        lostGuess = -1;
        container.children[i].classList.toggle("squareLose");
      }
      container.children[i].innerHTML = lostGame
        ? '<i class="fas fa-bomb"></i>'
        : "?";
    } else {
      container.children[i].innerHTML = "?";
    }
  }
}