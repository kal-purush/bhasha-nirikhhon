// The universe of the Game of Life is an infinite, two-dimensional orthogonal grid of square cells, each of which is in one of two possible states, alive or dead, (or populated and unpopulated, respectively). Every cell interacts with its eight neighbours, which are the cells that are horizontally, vertically, or diagonally adjacent. At each step in time, the following transitions occur:

// Any live cell with fewer than two live neighbours dies, as if by underpopulation.
// Any live cell with two or three live neighbours lives on to the next generation.
// Any live cell with more than three live neighbours dies, as if by overpopulation.
// Any dead cell with exactly three live neighbours becomes a live cell, as if by reproduction.

var canvas;
var matrixXsize = 0;
var matrixYsize = 0;
var x = 0;
var y = 0;
var sizeDice = 50;
var dotSize = 0;
var quarter = sizeDice/4;
var counter = 0;
var dices = [];
var max = 0;
var once = true;

function setup() {
// Sets the screen to be 720 pixels wide and 400 pixels high
// createCanvas(720, 400);
canvas = createCanvas(window.innerWidth, window.innerHeight);
dotSize = int(sizeDice/6);
background(0);
noStroke();
rectMode(CENTER); // Set ellipseMode is CORNER
ellipseMode(CENTER); // Set ellipseMode is CORNER
matrixXsize= int((window.innerWidth)/sizeDice);
matrixYsize= int((window.innerHeight)/sizeDice);
max = matrixYsize*matrixXsize;
console.log("X:", matrixXsize);
console.log("Y:", matrixYsize);
matrixYsize= (window.innerHeight)/sizeDice;
for (let i = 0; i < matrixXsize; i++) {
    for (let j = 0; j < matrixYsize; j++) {
    var n = new Dice(i*sizeDice+(sizeDice/2), j*sizeDice+(sizeDice/2), sizeDice, int(random(1,7)));
    n.display();
    }
}
}
function draw() {
    counter = dices.length-1;
    var x = ((counter)%matrixXsize);
    var y = floor(counter/matrixXsize);
    var n = new Dice(x*sizeDice+(sizeDice/2), y*sizeDice+(sizeDice/2), sizeDice, dices[dices.length-1]);
    n.display();
   
    if(counter > max){
        background(0);
        dices = [];
    }
}

function keyPressed() {
    console.log("KeyPressed");
    if (keyCode === DELETE) {
        reset();
    }
    if (keyCode === BACKSPACE) {
        newRandomCanvas();
    }if(keyCode === ENTER) {
        addRandomDice();
    }
}
function mouseClicked() {
    console.log("MouseClicked");
    addRandomDice();
}
function reset(){
    background(0);
    dices = [];
    once = true;
}
function addRandomDice(){
    dices.push(int(random(1,7)));

    if(once){
        once = false;
        background(0);
        // var n = new Dice((sizeDice/2), (sizeDice/2), sizeDice, dices[dices.length-1]);
        // n.display();
    }
}
function newRandomCanvas(){
    reset();
    for (let i = 0; i < matrixXsize; i++) {
        for (let j = 0; j < matrixYsize; j++) {
        var n = new Dice(i*sizeDice+(sizeDice/2), j*sizeDice+(sizeDice/2), sizeDice, int(random(1,7)));
        n.display();
        }
    }
}
// Dice class
class Dice {
    constructor(xx,yy,ss,id) {
      this.x = xx;
      this.y = yy;
      this.size = ss;
      this.id = id;
    }
  
    update() {

    }
  
    display() {
        fill(0);
        rect(this.x, this.y, this.size, this.size);
        if(this.id == 1){
        fill(255);
        ellipse(this.x, this.y, dotSize, dotSize);
        }
        else if(this.id == 2){
        fill(255);
        ellipse(this.x+quarter, this.y+quarter, dotSize, dotSize);
        ellipse(this.x-quarter, this.y-quarter, dotSize, dotSize);
        }
        else if(this.id == 3){
            fill(255);
            ellipse(this.x, this.y, dotSize, dotSize);
            ellipse(this.x+quarter, this.y+quarter, dotSize, dotSize);
            ellipse(this.x-quarter, this.y-quarter, dotSize, dotSize);
        }
        else if(this.id == 4){
            fill(255);
            ellipse(this.x+quarter, this.y+quarter, dotSize, dotSize);
            ellipse(this.x-quarter, this.y-quarter, dotSize, dotSize);            ellipse(this.x+quarter, this.y+quarter, dotSize, dotSize);
            ellipse(this.x-quarter, this.y+quarter, dotSize, dotSize);            ellipse(this.x+quarter, this.y+quarter, dotSize, dotSize);
            ellipse(this.x+quarter, this.y-quarter, dotSize, dotSize);
        }
        else if(this.id == 5){
            fill(255);
            ellipse(this.x, this.y, dotSize, dotSize);
            ellipse(this.x+quarter, this.y+quarter, dotSize, dotSize);
            ellipse(this.x-quarter, this.y-quarter, dotSize, dotSize);            ellipse(this.x+quarter, this.y+quarter, dotSize, dotSize);
            ellipse(this.x-quarter, this.y+quarter, dotSize, dotSize);            ellipse(this.x+quarter, this.y+quarter, dotSize, dotSize);
            ellipse(this.x+quarter, this.y-quarter, dotSize, dotSize);
        }
        else if(this.id == 6){
            fill(255);
            ellipse(this.x+quarter, this.y+quarter, dotSize, dotSize);
            ellipse(this.x-quarter, this.y-quarter, dotSize, dotSize);            ellipse(this.x+quarter, this.y+quarter, dotSize, dotSize);
            ellipse(this.x-quarter, this.y+quarter, dotSize, dotSize);            ellipse(this.x+quarter, this.y+quarter, dotSize, dotSize);
            ellipse(this.x+quarter, this.y-quarter, dotSize, dotSize);            ellipse(this.x-quarter, this.y+quarter, dotSize, dotSize);            ellipse(this.x+quarter, this.y+quarter, dotSize, dotSize);
            ellipse(this.x+quarter, this.y, dotSize, dotSize);            ellipse(this.x-quarter, this.y+quarter, dotSize, dotSize);            ellipse(this.x+quarter, this.y+quarter, dotSize, dotSize);
            ellipse(this.x-quarter, this.y, dotSize, dotSize);
        }
    }
  }