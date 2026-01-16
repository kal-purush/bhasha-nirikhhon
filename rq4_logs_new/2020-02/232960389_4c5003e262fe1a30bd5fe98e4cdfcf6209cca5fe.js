// Creating a model controller for the 2048 game 

// Here is a sample game state, for a 2048 game in play
var grid = [[2,'',4,''],['',4,8,16],[2,8,16,64],[2,'','','']];
var score = 110; 
var gameWon = false;

// This function returns the value of each location in the grid
function getValue(row,column){
    if (row > grid.length || column > grid[row].length){
        return "Inputted coordinates out of range";
    }
    else{
        return grid[row][column];
    } 
}

// This function returns the current score
function showScore(){
    return score
}

// This function adds values to the total score 
function addScore(added_score){
    score += added_score;
}

// This function adds columns up if they have the same value, and adds that 
// to the total score 
function addColumnUp(){
    for (let i = 0; i < grid.length; i++){
        for (let j = 0; j <= grid.length; j++){
            if (grid[i][j] == grid[i + 1][j]){
                let tempValue = grid[i][j] + grid[i + 1][j];
                grid[i][j] = tempValue;
                addScore(tempValue);
                grid[i+1][j] = ''; 
            }
        }
    }
    winningCondition();
}

// This functions adds columns down if they have the same value, and adds that
// to the total score 
function addColumnDown(){
    for (let i = 0; i < grid.length; i++){
        for (let j = 0; j <= grid.length; j++){
            if (grid[i][j] == grid[i + 1][j]){
                let tempValue = grid[i][j] + grid[i + 1][j];
                grid[i + 1][j] = tempValue;
                addScore(tempValue);
                grid[i][j] = '';
            }
        }
    }
    winningCondition();
}

// This function adds rows to the right, and adds that to the total score 
function addRowRight(){
    for (let i = 0; i <= grid.length; i++){
        for (let j = 0; j < grid.length; j++){
            if (grid[i][j] == grid[i][j + 1]){
                let tempValue = grid[i][j] + grid[i][j + 1];
                grid[i][j + 1] = tempValue;
                addScore(tempValue);
                grid[i][j] = '';
            }
        }
    }
    winningCondition();
}

// This function adds rows to the left, and adds that to the total score 
function addRowLeft(){
    for (let i = 0; i <= grid.length; i ++){
        for (let j = 0; j < grid.length; j ++){
            if (grid[i][j] == grid[i][j + 1]){
                let tempValue = grid[i][j] + grid[i][j + 1];
                grid[i][j] = tempValue;
                addScore(tempValue);
                grid[i][j + 1] = '';
            }
        }
    }
    winningCondition();
}

// This function checks to see if the game has been won 
function winningCondition(){
    for (let i = 0; i <= grid.length; i ++){
        for (let j = 0; j <= grid.length; j ++){
            if (grid[i][j] == 2048){
                gameWon = true;
            }
        }
    }
}