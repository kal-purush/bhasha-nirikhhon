// Start Game events and data changes
const button = document.querySelectorAll('div>button');
var player1, player2;
button[0].addEventListener('click', submitFunc);

function submitFunc() {
    const player = document.querySelectorAll('div>input');
    
    player1 = player[0].value;
    player2 = player[1].value;
    if(player1&&player2) {
        const inputCont = document.getElementById("input-cont");
        const gridCont = document.getElementById("grid-cont");
        inputCont.style.display = 'none';
        gridCont.style.display = 'flex';

        gameStart();
    }
    else {
        alert('Please enter both the player names');
    }
}

function gameStart() {
    const subHeading = document.getElementsByClassName('message');
    
    subHeading[0].innerText = player1+", you're up";
}

var turn = 0;
var gameOn = true;
var count = 0;

const one = document.getElementById('1');
const two = document.getElementById('2');
const three = document.getElementById('3');
const four = document.getElementById('4');
const five = document.getElementById('5');
const six = document.getElementById('6');
const seven = document.getElementById('7');
const eight = document.getElementById('8');
const nine = document.getElementById('9');

one.addEventListener('click', placeYourPick);
two.addEventListener('click', placeYourPick);
three.addEventListener('click', placeYourPick);
four.addEventListener('click', placeYourPick);
five.addEventListener('click', placeYourPick);
six.addEventListener('click', placeYourPick);
seven.addEventListener('click', placeYourPick);
eight.addEventListener('click', placeYourPick);
nine.addEventListener('click', placeYourPick);


function placeYourPick(event) {
    if(count!=9&&gameOn) {
        let playerMark = 'O';
        if(turn==0) {
            playerMark = 'X'
        }
        if(event.srcElement.innerText) return;
        event.srcElement.innerText = playerMark;
        count++;

        const subHeading = document.getElementsByClassName('message');
        let won = checkForWin();
        if(won) {
            gameOn = false;            
            if(turn) {
                subHeading[0].innerText = player2+" congratulations you won!";
            }
            else {
                subHeading[0].innerText = player1+" congratulations you won!";           
            }
        }
		else {
			if(count==9) {
                subHeading[0].innerText = "Oops! no places left, neither won";
            }
			else if(turn) {
				subHeading[0].innerText = player1+", you're up";
			}
			else subHeading[0].innerText = player2+", you're up";
		}

        if(turn) {
            turn = 0;
        }
        else turn++;
    }
    else {
        const subHeading = document.getElementsByClassName('message');
        subHeading[0].innerText = "Game Completed, click on restart for a new game";        
    }
}
function checkForWin() {
    if(one.innerText=='X'&&two.innerText=='X'&&three.innerText=='X') {
        return true;
    }
    if(one.innerText=='X'&&four.innerText=='X'&&seven.innerText=='X') {
        return true;
    }
    if(four.innerText=='X'&&five.innerText=='X'&&six.innerText=='X') {
        return true;
    }
    if(seven.innerText=='X'&&eight.innerText=='X'&&nine.innerText=='X') {
        return true;
    }
    if(five.innerText=='X'&&two.innerText=='X'&&eight.innerText=='X') {
        return true;
    }
    if(six.innerText=='X'&&nine.innerText=='X'&&three.innerText=='X') {
        return true;
    }
    if(one.innerText=='X'&&five.innerText=='X'&&nine.innerText=='X') {
        return true;
    }
    if(three.innerText=='X'&&five.innerText=='X'&&seven.innerText=='X') {
        return true;
    }
    
    if(one.innerText=='O'&&two.innerText=='O'&&three.innerText=='O') {
        return true;
    }
    if(one.innerText=='O'&&four.innerText=='O'&&seven.innerText=='O') {
        return true;
    }
    if(four.innerText=='O'&&five.innerText=='O'&&six.innerText=='O') {
        return true;
    }
    if(seven.innerText=='O'&&eight.innerText=='O'&&nine.innerText=='O') {
        return true;
    }
    if(five.innerText=='O'&&two.innerText=='O'&&eight.innerText=='O') {
        return true;
    }
    if(six.innerText=='O'&&nine.innerText=='O'&&three.innerText=='O') {
        return true;
    }
    if(one.innerText=='O'&&five.innerText=='O'&&nine.innerText=='O') {
        return true;
    }
    if(three.innerText=='O'&&five.innerText=='O'&&seven.innerText=='O') {
        return true;
    }
    return false;
}

button[1].addEventListener('click', clearBoard);
button[2].addEventListener('click',restartGame);

function clearBoard() {
    count = 0;
    one.innerText = '';
    two.innerText = '';
    three.innerText = '';
    four.innerText = '';
    five.innerText = '';
    six.innerText = '';
    seven.innerText = '';
    eight.innerText = '';
    nine.innerText = '';
    turn = 0;
    gameOn = true;		
    const subHeading = document.getElementsByClassName('message');
    subHeading[0].innerText = player1+", you're up";    
}

function restartGame() {
    location.reload();
    // const inputCont = document.getElementById("input-cont");
    // const gridCont = document.getElementById("grid-cont");
    // inputCont.style.display = 'block';
    // gridCont.style.display = 'none';
    // clearBoard();
}