
var canvas = document.getElementById("mazecanvas");
var context = canvas.getContext("2d");

//monsters's pixel position
var monsterX1 = 100;
var monsterY1 =  22;
var monsterX2 = 110;
var monsterY2 =  294;
var monsterX3 = 585;
var monsterY3 =  73;
var monsterX4 = 360;
var monsterY4 =  447;

var monsterX5 = 660;
var monsterY5 = 481;
var monsterX6 = 415;
var monsterY6 = 243;


var myRectX = 840;
var myRectY = 380;

var mazeWidth = 856;
var mazeHeight = 542;
var badPixels = [];
var intervalVar;
var timer = 300;
var gameState = true;


window.onload = function() {		
    drawMaze();

    window.addEventListener("keydown", moveMyRect, true);
	runAI(monsterX1, monsterY1, 4, true);
	runAI(monsterX2, monsterY2, 4, true);
	runAI(monsterX3, monsterY3, 4, true);
	runAI(monsterX4, monsterY4, 4, true);
	runAI(monsterX5, monsterY5, 4, true);
	runAI(monsterX6, monsterY6, 4, true);

    createTimer(timer); //5 min timer
};
function drawMaze() {
    makeWhite(0, 0, canvas.width, canvas.height);
    var mazeImg = new Image();
    mazeImg.onload = function () {
        context.drawImage(mazeImg, 0, 0);
        drawRectangleMine(myRectX, myRectY, '#00FF00');
    };
    mazeImg.src = "img/maze6.gif";
}
function runAI(X, Y, key, firsttime) {
	var result;
	var keyUsed = "";
	var possibleKey;
	var isDeadendPoint;
	if(firsttime) {
		possibleKey = false;
		isDeadendPoint = false;
		moveRect(X, Y, key, true);
		var delay=200;
	
		setTimeout(function(){	
				runAI(X, Y, key, false);
		},delay);

	} else {
		var delay=1;
		possibleKey = checkOtherDirections(X, Y, key);
		isDeadendPoint = isDeadend(X, Y, key);		
		
		if(isDeadendPoint) {
			if(key == 1) {
				result = moveRect(X, Y, 2, false);
				keyUsed = 2;		
			}
			else if (key == 2) {
				result = moveRect(X, Y, 1, false);
				keyUsed = 1;
	
			}
			else if (key == 3) {
				result = moveRect(X, Y, 4, false);
				keyUsed = 4;
			}
			else if (key == 4) {
				result = moveRect(X, Y, 3, false);
				keyUsed = 3;
			}								
		}
		else {
			if(checkDirection(X, Y, key) == 1) {
				if(possibleKey != false){				
					possibleKeyArray = [key, possibleKey];
					var i = Math.floor((Math.random() * 2) + 0);
					result = moveRect(X, Y, possibleKeyArray[i], false);
					keyUsed = possibleKeyArray[i];
				}else {
					keyUsed = key;
					result = moveRect(X, Y, key, false);
				}
			}
			else {
				result = moveRect(X, Y, possibleKey, false);
				keyUsed = possibleKey;
			}				
		}
		if(gameState) {
			setTimeout(function(){	
				if(result[0] == -1 || result[1] == -1) {
					runAI(X, Y, keyUsed, false);
				} else {
					runAI(result[0], result[1], keyUsed, false);
				}
			},delay);			
		}

	}//end first if that checks if ran for first time
	
}

function drawRectangle(X, Y, style) {
    context.beginPath();
    context.rect(X, Y, 15, 15);
    context.closePath();
    context.fillStyle = style;
    context.fill();
}
function drawRectangleMine(X, Y, style) {
    makeWhite(X, Y, 10, 10);
    context.fillStyle = style;
    context.fillRect(X, Y, 10, 10);
}        
function moveRect(X, Y, key) {
    var newX;
    var newY;
    var movingAllowed;
    //e = e || window.event;
    switch (key) {
        case 1: // up
            newX = X;
            newY = Y - 1;
            break;
        case 3: // left
            newX = X - 1;
            newY = Y;
            break;
        case 2: // down
            newX = X;
            newY = Y + 1;
            break;
        case 4: // right
            newX = X + 1;
            newY = Y;
            break;
    }
    movingAllowed = canMoveTo(newX, newY);
    
    if (movingAllowed === 1) {      // 1 means 'the rectangle can move'
    	makeWhite(X, Y, 15, 15);
        drawRectangle(newX, newY, "#0000FF");
        var result = [newX, newY];
        return result;
    }

    else {
    	return [-1, -1];
    }
	
}
function canMoveTo(destX, destY, firsttime) {
	
	if(firsttime) {
		return 1;
	}
	else {
	    var imgData = context.getImageData(destX, destY, 15, 15);
	    var data = imgData.data;
	    var canMove = 1; // 1 means: the rectangle can move
	    if (destX >= 0 && destX <= mazeWidth - 15 && destY >= 0 && destY <= mazeHeight - 15) {
	        for (var i = 0; i < 4 * 15 * 15; i += 4) {
	            if (data[i] === 0 && data[i + 1] === 0 && data[i + 2] === 0) { // black
	                canMove = 0; // 0 means: the rectangle can't move
	                break;
	            }
	            else if (data[i] === 0 && data[i + 1] === 255 && data[i + 2] === 0) { // #00FF00
	                canMove = 2; // 2 means: monster killed you
	                yourDead();
	                break;
	            }

	        }
	    }
	    else {
	        canMove = 0;
	    }
	    return canMove;		
	}
}
function canMoveToPlayer(destX, destY) {
	
    var imgData = context.getImageData(destX, destY, 10, 10);
    var data = imgData.data;
    var canMove = 1; // 1 means: the rectangle can move
    if (destX >= 0 && destX <= mazeWidth - 15 && destY >= 0 && destY <= mazeHeight - 15) {
        for (var i = 0; i < 4 * 15 * 15; i += 4) {
            if (data[i] === 0 && data[i + 1] === 0 && data[i + 2] === 0) { // black
                canMove = 0; // 0 means: the rectangle can't move
                break;
            }
            else if (data[i] === 0 && data[i + 1] === 0 && data[i + 2] === 255) { // #00FF00
                canMove = 2; // 2 means: monster killed you
                break;
            }
            else if (data[i] === 255 && data[i + 1] === 0 && data[i + 2] === 0) { // #FF0000
            	canMove = 3; // 2 means: you won
            	youWon();
            	break;
            }
        }
    }
    else {
        canMove = 0;
    }
    return canMove;
}        
function createTimer(seconds) {
    intervalVar = setInterval(function () {
        if (seconds === 0) {
        	yourDead();
            return;
        }
        var minutes = Math.floor(seconds / 60);
        var secondsToShow = (seconds - minutes * 60).toString();
        if (secondsToShow.length === 1) {
            secondsToShow = "0" + secondsToShow; // if the number of seconds is '5' for example, make sure that it is shown as '05'
        }
		document.getElementById("timer").innerHTML = minutes.toString() + ":" + secondsToShow;
        seconds--;
    }, 1000);
}
function checkBadPixelArray(pix) {
	if(badPixels.indexOf(pix) == -1) return false;
	else return true;
}
function makeWhite(x, y, w, h) {
    context.beginPath();
    context.rect(x, y, w, h);
    context.closePath();
    context.fillStyle = "#FFFFFF";
    context.fill();
}
function makeBlack(x, y, w, h) {
    context.beginPath();
    context.rect(x, y, w, h);
    context.closePath();
    context.fillStyle = "#000000";
    context.fill();
}
function checkDirection (X, Y, key) {
	var testX, testY;
	
	switch (key) {
        case 1: // up
            testX = X;
            testY = Y - 1;
            break;
        case 3: // A key
            testX = X - 1;
            testY = Y;
            break;
        case 2: // S key
            testX = X;
            testY = Y + 1;
            break;
        case 4: // arrow right key
            testX = X + 1;
            testY = Y;
            break;
    }
    var movingAllowed = canMoveTo(testX, testY);
	return movingAllowed;
}

function checkOtherDirections(X, Y, dir) {
	if(dir == 4) {
		if(checkDirection(X, Y, 1) == 1) {
			return 1;
		}
		if(checkDirection(X, Y, 2) == 1) {
			return 2;
		}
	}
	if(dir == 3) {
		if(checkDirection(X, Y, 1) == 1) {
			return 1;
		}
		if(checkDirection(X, Y, 2) == 1) {
			return 2;
		}
	
	}
	 if(dir == 2) {

		if(checkDirection(X, Y, 3) == 1) {
			return 3;
		}
		if(checkDirection(X, Y, 4) == 1) {
			return 4;
		}
		
	}
	if(dir == 1) {
		if(checkDirection(X, Y, 4) == 1) {
			return 4;
		}

		if(checkDirection(X, Y, 3) == 1) {
			return 3;
		}
	}
	return false;
}
function isDeadend(X, Y, dir){
	if(dir == 1) {
		if(checkDirection(X, Y, 1) != 1 && checkDirection(X, Y, 3) != 1  && checkDirection(X, Y, 4) != 1) {
			return true;
		}
	}
	if(dir == 2) {
		if(checkDirection(X, Y, 2) != 1 && checkDirection(X, Y, 3) != 1  && checkDirection(X, Y, 4) != 1) {
			return true;
		}
	}
	if(dir == 3) {
		if(checkDirection(X, Y, 1) != 1 && checkDirection(X, Y, 2) != 1  && checkDirection(X, Y, 3) != 1) {
			return true;
		}
	}
	if(dir == 4) {
		if(checkDirection(X, Y, 1) != 1 && checkDirection(X, Y, 2) != 1  && checkDirection(X, Y, 4) != 1) {
			return true;
		}
	}			
	return false;
	
}

function moveFirstTime(X, Y, key) {
	result = moveRect(X, Y, key);
	keyUsed = key;
	setTimeout(function(){
		if(result[0] == -1 || result[1] == -1) {
			runAI(X, Y, keyUsed);
		} else {
			runAI(result[0], result[1], keyUsed);
		}
		
	},delay);
}
function moveMyRect (e) {
	e = e || window.event;
	switch (e.keyCode) {
        case 38:   // arrow up key
        case 87: // W key
            newX = myRectX;
            newY = myRectY - 3;
            break;
        case 37: // arrow left key
        case 65: // A key
            newX = myRectX - 3;
            newY = myRectY;
            break;
        case 40: // arrow down key
        case 83: // S key
            newX = myRectX;
            newY = myRectY + 3;
            break;
        case 39: // arrow right key
        case 68: // D key
            newX = myRectX +3;
            newY = myRectY;
            break;
    }
    movingAllowed = canMoveToPlayer(newX, newY);
    if (movingAllowed === 1) {      // 1 means 'the rectangle can move'
        drawRectangleMine(myRectX, myRectY, "#FFFFFF");
        drawRectangleMine(newX, newY, "#00FF00");
        myRectX = newX;
        myRectY = newY;
    }
    else if (movingAllowed === 2) { // 2 means 'the rectangle reached the end point'
        yourDead();
        window.removeEventListener("keydown", moveMyRect, true);
    }
}
function yourDead() {
	gameState = false;
    clearInterval(intervalVar);
    makeBlack(0, 0, canvas.width, canvas.height);
    context.font = "40px Arial";
    context.fillStyle = "white";
    context.textAlign = "center";
    context.textBaseline = "middle";
    context.fillText("You're Dead!", canvas.width / 2, canvas.height / 2);
}
function youWon() {
	gameState = false;
    clearInterval(intervalVar);
    makeBlack(0, 0, canvas.width, canvas.height);
    context.font = "40px Arial";
    context.fillStyle = "white";
    context.textAlign = "center";
    context.textBaseline = "middle";
    context.fillText("You Won!", canvas.width / 2, canvas.height / 2);
}
		
		