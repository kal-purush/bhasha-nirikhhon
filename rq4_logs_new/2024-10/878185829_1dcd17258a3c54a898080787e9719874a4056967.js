
document.addEventListener('DOMContentLoaded', () => {
    const gameBoard = document.getElementById('game-board');
    const cells = document.querySelectorAll('.cell');
    const resetButton = document.getElementById('reset-button');
    const leaderboardList = document.getElementById('leaderboard-list');
    let currentPlayer = 'X';
    let gameActive = true;
    let startTime;

    function loadLeaderboard() {
        const leaderboard = JSON.parse(localStorage.getItem('leaderboard')) || [];
        leaderboardList.innerHTML = '';
        leaderboard.slice(0, 10).forEach(entry => {
            const li = document.createElement('li');
            li.textContent = `${entry.name} - ${entry.time} segundos - ${entry.date}`;
            leaderboardList.appendChild(li);
        });
    }

    function saveLeaderboard(name, time) {
        const leaderboard = JSON.parse(localStorage.getItem('leaderboard')) || [];
        const date = new Date().toLocaleString();
        leaderboard.push({ name, time, date });
        leaderboard.sort((a, b) => a.time - b.time);
        localStorage.setItem('leaderboard', JSON.stringify(leaderboard));
        loadLeaderboard();
    }

    function handleCellClick(e) {
        const cell = e.target;
        if (cell.textContent !== '' || !gameActive) {
            return;
        }

        if (!startTime) {
            startTime = new Date();
        }

        cell.textContent = currentPlayer;
        if (checkWinner()) {
            gameActive = false;
            const endTime = new Date();
            const timeTaken = Math.floor((endTime - startTime) / 1000);
            alert(`¡Ganaste en ${timeTaken} segundos!`);
            const playerName = prompt('Ingresa tu nombre para guardar el puntaje:');
            if (playerName) {
                saveLeaderboard(playerName, timeTaken);
            }
        } else if (isDraw()) {
            gameActive = false;
            alert('¡Empate!');
        } else {
            currentPlayer = currentPlayer === 'X' ? 'O' : 'X';
            if (currentPlayer === 'O') {
                computerMove();
            }
        }
    }

    function computerMove() {
        const emptyCells = Array.from(cells).filter(cell => cell.textContent === '');
        if (emptyCells.length === 0) {
            return;
        }
        const randomIndex = Math.floor(Math.random() * emptyCells.length);
        emptyCells[randomIndex].textContent = 'O';
        if (checkWinner()) {
            gameActive = false;
            alert('¡La computadora ganó!');
        } else if (isDraw()) {
            gameActive = false;
            alert('¡Empate!');
        } else {
            currentPlayer = 'X';
        }
    }

    function checkWinner() {
        const winningCombinations = [
            [0, 1, 2],
            [3, 4, 5],
            [6, 7, 8],
            [0, 3, 6],
            [1, 4, 7],
            [2, 5, 8],
            [0, 4, 8],
            [2, 4, 6]
        ];

        return winningCombinations.some(combination => {
            const [a, b, c] = combination;
            return cells[a].textContent !== '' &&
                   cells[a].textContent === cells[b].textContent &&
                   cells[a].textContent === cells[c].textContent;
        });
    }

    function isDraw() {
        return Array.from(cells).every(cell => cell.textContent !== '');
    }

    function resetGame() {
        cells.forEach(cell => cell.textContent = '');
        currentPlayer = 'X';
        gameActive = true;
        startTime = null;
    }

    cells.forEach(cell => cell.addEventListener('click', handleCellClick));
    resetButton.addEventListener('click', resetGame);
    loadLeaderboard();
});