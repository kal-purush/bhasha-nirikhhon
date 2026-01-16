/*
  Connect 4 Game
  Author: Jeremiah Johnson
  Date: March 07, 2024
  Description: Classic Connect 4 game where two players take turns dropping discs into a grid.
*/

const columns = 7;
const rows = 6;
const board = [];
let currentPlayer = 1;

// Function to create the game board in the DOM
function createBoard() {
    const gameBoard = document.getElementById('gameBoard');
    for (let row = 0; row < rows; row++) {
        const rowElements = [];
        for (let col = 0; col < columns; col++) {
            const cell = document.createElement('div');
            cell.className = 'cell';
            cell.addEventListener('click', () => placeDisc(col));
            gameBoard.appendChild(cell);
            rowElements.push({ cell, row, col, player: 0 });
        }
        board.push(rowElements);
    }
}

// Function to handle placing a disc in a column
function placeDisc(col) {
    for (let row = rows - 1; row >= 0; row--) {
        if (board[row][col].player === 0) {
            board[row][col].player = currentPlayer;
            board[row][col].cell.classList.add(`player${currentPlayer}`);
            currentPlayer = currentPlayer === 1 ? 2 : 1;
            document.getElementById('gameStatus').innerText = `Player ${currentPlayer}'s Turn`;
            break;
        }
    }
}

function checkForWin() {
    // Check horizontal
    for (let row = 0; row < rows; row++) {
        for (let col = 0; col < columns - 3; col++) {
            if (board[row][col].player === currentPlayer &&
                board[row][col + 1].player === currentPlayer &&
                board[row][col + 2].player === currentPlayer &&
                board[row][col + 3].player === currentPlayer) {
                return true;
            }
        }
    }

    // Check vertical
    for (let col = 0; col < columns; col++) {
        for (let row = 0; row < rows - 3; row++) {
            if (board[row][col].player === currentPlayer &&
                board[row + 1][col].player === currentPlayer &&
                board[row + 2][col].player === currentPlayer &&
                board[row + 3][col].player === currentPlayer) {
                return true;
            }
        }
    }

    // Check diagonal (down-right and up-right)
    for (let row = 0; row < rows; row++) {
        for (let col = 0; col < columns; col++) {
            // Down-right
            if (row <= rows - 4 && col <= columns - 4 &&
                board[row][col].player === currentPlayer &&
                board[row + 1][col + 1].player === currentPlayer &&
                board[row + 2][col + 2].player === currentPlayer &&
                board[row + 3][col + 3].player === currentPlayer) {
                return true;
            }

            // Up-right
            if (row >= 3 && col <= columns - 4 &&
                board[row][col].player === currentPlayer &&
                board[row - 1][col + 1].player === currentPlayer &&
                board[row - 2][col + 2].player === currentPlayer &&
                board[row - 3][col + 3].player === currentPlayer) {
                return true;
            }
        }
    }

    return false;
}

function placeDisc(col) {
    for (let row = rows - 1; row >= 0; row--) {
        if (board[row][col].player === 0) {
            board[row][col].player = currentPlayer;
            board[row][col].cell.classList.add(`player${currentPlayer}`);
            const hasWon = checkForWin();
            if (hasWon) {
                document.getElementById('gameStatus').innerText = `Player ${currentPlayer} Wins!`;
                document.getElementById('restartButton').style.display = 'block'; // Show the restart button
                disableBoard();
                return;
            }
            currentPlayer = currentPlayer === 1 ? 2 : 1;
            document.getElementById('gameStatus').innerText = `Player ${currentPlayer}'s Turn`;
            break;
        }
    }
}

// Function to disable the game board after a win
function disableBoard() {
    for (let row = 0; row < rows; row++) {
        for (let col = 0; col < columns; col++) {
            board[row][col].cell.removeEventListener('click', placeDisc);
        }
    }
}

// Function to restart the game
function restartGame() {
    // Clear the board
    board.forEach(row => row.forEach(cell => {
        cell.player = 0;
        cell.cell.className = 'cell'; // Reset class name
    }));

    // Reset the current player to 1
    currentPlayer = 1;
    document.getElementById('gameStatus').innerText = "Player 1's Turn";

    // Hide the restart button
    document.getElementById('restartButton').style.display = 'none';

}

// Function to handle cell click
function onCellClick() {
    const col = Array.from(this.parentNode.children).indexOf(this);

    if (col !== -1) {
        placeDisc(col);

        if (checkForWin()) {
            document.getElementById('gameStatus').innerText = `Player ${currentPlayer} wins!`;
            disableBoard();
            document.getElementById('restartButton').style.display = 'block';
        }
    } else {
        console.error('Error: Invalid column index.');
    }
}

// Add an event listener for the rules button
document.getElementById('rulesButton').addEventListener('click', toggleRules);

// Function to toggle the display of rules section
function toggleRules() {
    const rulesSection = document.getElementById('rulesSection');
    rulesSection.style.display = rulesSection.style.display === 'none' ? 'block' : 'none';
}

// Add an event listener for the restart button
document.getElementById('restartButton').addEventListener('click', restartGame);

// Event listener to create the board when the window loads
window.onload = () => {
    createBoard(); // Make sure this doesn't duplicate the board if called again
};