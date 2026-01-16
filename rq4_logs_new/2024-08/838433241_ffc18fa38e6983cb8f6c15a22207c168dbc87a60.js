/*
think
1. Button click - Attach Event on Button
Event (click, keypress, drag/drop)
Listener
Action
Ghar ki bell baji, hamne suni hamesha sunte hain, fir ham action karte hain
2. Print X or 0
We have 9 buttons, we need to attach event on all 9 buttons, need to listen the events and then take the action

how i find out which button is clicked out of the 9 buttons, becasue every button is calling the same function

Add - Find out the Win and Lose - 8 condition
Find out the Draw Condition - all cell filled/no more can be filled
Build the reset function
Once game is over reset after 5 sec, it show countdown then reset

*/


let currentActive = "X";
let notActive = "O";

const buttons = document.getElementsByTagName('button');
const displayResult = document.getElementsByTagName('p');
for (let i=0; i<buttons.length; i++){
    buttons[i].addEventListener('click', printXorZero);
}

const resetButton = document.getElementById('reset');
resetButton.addEventListener('click', reset);

let winningConditions = [
    ["cell-0", "cell-3", "cell-6"],
    ["cell-6", "cell-7", "cell-8"],
    ["cell-2", "cell-5", "cell-8"],
    ["cell-0", "cell-1", "cell-2"],
    ["cell-0", "cell-4", "cell-8"],
    ["cell-2", "cell-4", "cell-6"],
    ["cell-3", "cell-4", "cell-5"],
    ["cell-1", "cell-4", "cell-7"]
]

let flag= true;
let count = 0;
let countdownInterval;
let gameOver = false;

function printXorZero(){
    // this - keyword (current calling object reference)

    if (gameOver) return;

    console.log('printXorZero calls', this);
    const currentButton = this;
    
    if (currentButton.innerText.length==0){
        currentButton.innerText = flag?"X":"O";
        flag = !flag;
        count++;
        
        if (count==9){
            draw();
        }

        checkWinLose(currentButton);
        //The checking of Win or Loseif ()
        
    }
    
}

function checkWinLose(currentButton){
    for(let i=0; i<winningConditions.length; i++){
        if (document.getElementById(winningConditions[i][0]).innerText == document.getElementById(winningConditions[i][1]).innerText && document.getElementById(winningConditions[i][1]).innerText == document.getElementById(winningConditions[i][2]).innerText && document.getElementById(winningConditions[i][1]).innerText){
            displayResult[0].innerText = currentButton.innerText + " Wins";
            gameOver = true;
            startCountdown();
            return;
        }
    }
}

function reset(){
    clearInterval(countdownInterval);

    for (let i=0; i<buttons.length; i++){
        buttons[i].innerText = "";
    }
    displayResult[0].innerText = "";
    displayResult[1].innerText = "";
    resetButton.innerText = "Reset";
    gameOver = false;
    flag = true;
    count = 0;
}

function draw(){
    displayResult[0].innerText = "Draw";
    count = 0;
    startCountdown();
}


let second = 5;

function startCountdown() {
    clearInterval(countdownInterval);
    displayResult[1].innerText = "Reset in " + second;
    countdownInterval = setInterval(countdown, 1000);
}

function countdown() {
    second--;
    if (second > 0) {
        displayResult[1].innerText = "Reset in " + second;
    } else {
        clearInterval(countdownInterval);
        second = 5;
        reset();
    }
}

/*
// https://dontpad.com/mern-bpit-code
/*
Think
1. Button click - Attach Event on Button
Event (click , keypress, drag/drop)
Listener
Action 
2. Print X or 0 
We have 9 buttons , we need to attach event on all 9 buttons , need
to listen the events and then take the action

how i find out which button is click out of 9 buttons, because every button
is calling the same function

Add - FInd out the Win and Lose 
Find out the Draw Condition
Build the reset feature
once game over reset after 5 sec, it show countdown then reset

const buttons = document.getElementsByTagName('button');
console.log('All Buttons ', buttons.length);
for(var i = 0; i<buttons.length; i++){
    buttons[i].addEventListener('click', printXorZero);
}
var turnCount = 0;
var flag = true;
// Arrow - Short Hand Function
// When function is of one line code - then use arrow function
// function isNotBlank(button){
//     return button.innerText.trim().length>0
// }
const isNotBlank = button =>button.innerText.trim().length>0;
const isThreeSame=(first, second, third)=>
first.innerText == second.innerText && first.innerText ==third.innerText && isNotBlank(first)
    

function isDraw(){
    if(turnCount==9){
        document.getElementById('result').innerText  = 'Game Draw';   
   }
}
function isGameOver(){
    const r= isThreeSame(buttons[0], buttons[3], buttons[6]) 
    || isThreeSame(buttons[0], buttons[1], buttons[2]) 
    || isThreeSame(buttons[3], buttons[4], buttons[5]) 
    || isThreeSame(buttons[6], buttons[7], buttons[8]) ||
     isThreeSame(buttons[3], buttons[4], buttons[5]) ||
      isThreeSame(buttons[6], buttons[7], buttons[8]) || 
        isThreeSame(buttons[0], buttons[4], buttons[8]) || 
        isThreeSame(buttons[2], buttons[4], buttons[6]);
    const winner= !flag;
    console.log(r);
    if(r){
        document.getElementById('result').innerText = winner?"X Wins":"0 Wins";
    }
    return r;
}
function printXorZero(){
    // this - keyword (current calling object reference )
    console.log('printXorZero calls', this);
    const currentButton = this;
    
    if(currentButton.innerText.length==0){
    currentButton.innerText = flag?"X":"0";
    flag = !flag;
    turnCount++;
    if(turnCount>=5){
        isGameOver() || isDraw();
        //isGameOver runs if not true then isDraw runs
        
    }
    
    }
}

*/