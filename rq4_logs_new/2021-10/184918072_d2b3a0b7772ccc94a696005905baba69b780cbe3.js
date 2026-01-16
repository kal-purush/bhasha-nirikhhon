var start, end;
var select = 0;
const feild = document.getElementById('grid');
const slider = document.getElementById('animation_speed');
var c = feild.getContext("2d");
c.fillStyle = "#0099ff";
var DONE = 0;
let dontAnimateBlank = 0;
let animation_go_on = 1;
let WIDTH = innerWidth;
let HEIGHT = innerHeight;
feild.width = feild.height = 800;
var cols = rows = 3;
var forMouseClick = feild.width / cols;
var w = feild.width / cols;
var h = feild.height / rows;
const pendingColor = 'rgba(14, 230, 14, 1.0)';
const startColor = 'rgb(252, 3, 198)';
const endColor = 'rgb(252, 111, 3)';
const processedColor = 'red';
const boardColor = 'rgb(85, 85, 240)';
const obstacleColor = 'rgb(120, 0, 24)';
let speed = 70;
var pendingPoints = [];
var processedPoints = [];
let LIFE = false;
let ifMouseDown = false;
let board = ['X', '', '', '', '', '', '', '', ''];


const result = () => {
    let states = []
    states.push(board.slice(0, 3).join());
    states.push(board.slice(3, 6).join());
    states.push(board.slice(6, 9).join());
    states.push(board[0] + board[3] + board[6]);
    states.push(board[1] + board[4] + board[7]);
    states.push(board[2] + board[5] + board[8]);
    states.push(board[0] + board[4] + board[8]);
    states.push(board[2] + board[4] + board[6]);

    states.forEach(el => {
        if (el === 'XXX'){
            return 'X'
        } else if (el === 'OOO') {
            return 'O'
        }
    });

    board.forEach(el => {
        if (el === '') {
            return 'tie';
        }
    });

    return null;
}

const minimax = (depth, isMax) => {
    const res = result();

    if (result) {
        if (res == human) {
            return -1;
        } else if (res == ai) {
            return 1;
        } else {
            return 0;
        }
    }

    if (isMax) {
        let bestScore = -Infinity;

        for (let i = 0; i < 9; i++) {
            if 
        }
    }
}

var final_path = [];

function createGrid(x, y) {
    var grid = new Array(x);
    for (var i = 0; i < grid.length; i++) {
        grid[i] = new Array(y);
    }

    for (var i = 0; i < x; i++) {
        for (var j = 0; j < y; j++) {
            grid[i][j] = new Point(i, j);
        }
    }

    return grid;
}

var grid = createGrid(cols, rows);
//console.log(grid);
function reset() {
    final_path = [];
    processedPoints = [];
    pendingPoints = [];
    DONE = 0;
    animation_go_on = 1;
}
function Point(x, y) {
    this.x = x;
    this.y = y;
    




    this.show = function (color) {
        c.fillStyle = color;
        c.strokeStyle = 'black';
        c.strokeRect(this.x * w, this.y * h, w - 1, h - 1);
        c.fillRect(this.x * w, this.y * h, w - 1, h - 1);
        c.textAlign = 'center';
        c.textBaseLine = 'middle';
        c.fillStyle = 'black';
        c.font = `${160}px Verdana`;
        console.log(board[this.x + this.y]);
        c.fillText(board[this.x + this.y], this.x * w + w/2, this.y * h + h/2);
    }

    

}

function reset_board() {
    window.location.reload();

}

function animate_begin() {
    setTimeout(() => {
        if (dontAnimateBlank != 1) {

            requestAnimationFrame(animate_begin);
            c.fillStyle = 'white';
            c.fillRect(0, 0, 800, 800);

            

            for (var i = 0; i < grid.length; i++) {
                for (var j = 0; j < grid[i].length; j++) {
                    if (grid[i][j].obstacle) {

                        grid[i][j].show(obstacleColor)

                    }
                    else {
                        grid[i][j].show(boardColor);
                    }

                }
            }
        }
    }, 250);

}
animate_begin();


feild.addEventListener('click', event => {
    let bound = feild.getBoundingClientRect();

    let x = Math.ceil((event.clientX - bound.left - feild.clientLeft) / forMouseClick);
    let y = Math.ceil((event.clientY - bound.top - feild.clientTop) / forMouseClick);
    //console.log(x, y);
    //console.log(x/25, y/25);

    if (select == 33) {
        select = 0;
        start = grid[x - 1][y - 1];
        grid[x - 1][y - 1].show(startColor);
    } else if (select == 66) {
        select = 0;
        end = grid[x - 1][y - 1];
        grid[x - 1][y - 1].show(endColor);
    } else {
        grid[x - 1][y - 1].obstacle = !grid[x - 1][y - 1].obstacle;
        // grid[x - 1][y - 1].alive = !grid[x - 1][y - 1].alive;

    }


});

feild.addEventListener('mousedown', event => {
    let bound = feild.getBoundingClientRect();

    let x = Math.ceil((event.clientX - bound.left - feild.clientLeft) / forMouseClick);
    let y = Math.ceil((event.clientY - bound.top - feild.clientTop) / forMouseClick);
    console.log('mousedown');
    ifMouseDown = true;
    //console.log(x/25, y/25);

    if (select == 33) {
        select = 0;
        start = grid[x - 1][y - 1];
        grid[x - 1][y - 1].show(startColor);
    } else if (select == 66) {
        select = 0;
        end = grid[x - 1][y - 1];
        grid[x - 1][y - 1].show(endColor);
    } else {
        grid[x - 1][y - 1].obstacle = !grid[x - 1][y - 1].obstacle;
        // grid[x - 1][y - 1].alive = !grid[x - 1][y - 1].alive;

    }


});

feild.addEventListener('mouseup', event => {
    let bound = feild.getBoundingClientRect();

    let x = Math.ceil((event.clientX - bound.left - feild.clientLeft) / forMouseClick);
    let y = Math.ceil((event.clientY - bound.top - feild.clientTop) / forMouseClick);
    console.log('mouseup');
    ifMouseDown = false;
    //console.log(x/25, y/25);

    if (select == 33) {
        select = 0;
        start = grid[x - 1][y - 1];
        grid[x - 1][y - 1].show(startColor);
    } else if (select == 66) {
        select = 0;
        end = grid[x - 1][y - 1];
        grid[x - 1][y - 1].show(endColor);
    } else {
        grid[x - 1][y - 1].obstacle = !grid[x - 1][y - 1].obstacle;
        // grid[x - 1][y - 1].alive = !grid[x - 1][y - 1].alive;

    }


});

feild.addEventListener('mousemove', event => {
    let bound = feild.getBoundingClientRect();

    let x = Math.ceil((event.clientX - bound.left - feild.clientLeft) / forMouseClick);
    let y = Math.ceil((event.clientY - bound.top - feild.clientTop) / forMouseClick);
    console.log(x, y);
    //console.log(x/25, y/25);

  if(ifMouseDown) {
      grid[x - 1][y - 1].obstacle = !grid[x - 1][y - 1].obstacle;
  } else if(!grid[x - 1][y - 1].obstacle) {

      grid[x - 1][y - 1].show('rgb(99, 99, 250)');
  }
   
});



