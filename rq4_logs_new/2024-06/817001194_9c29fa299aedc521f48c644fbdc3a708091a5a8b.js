const boardSize = 5;
const gameBoard = document.getElementById('game-board');
const discountInfo = document.getElementById('discount-info');
const character = { x: 0, y: 0 };
const shops = [
    { x: 1, y: 1, discount: '10% off' },
    { x: 3, y: 2, discount: '20% off' },
    { x: 4, y: 4, discount: '30% off' }
];

function createBoard() {
    for (let y = 0; y < boardSize; y++) {
        for (let x = 0; x < boardSize; x++) {
            const cell = document.createElement('div');
            cell.classList.add('cell');
            cell.dataset.x = x;
            cell.dataset.y = y;
            gameBoard.appendChild(cell);
        }
    }
    updateBoard();
}

function updateBoard() {
    document.querySelectorAll('.cell').forEach(cell => {
        cell.classList.remove('character');
        cell.classList.remove('shop');
    });
    const characterCell = document.querySelector(`.cell[data-x="${character.x}"][data-y="${character.y}"]`);
    characterCell.classList.add('character');
    shops.forEach(shop => {
        const shopCell = document.querySelector(`.cell[data-x="${shop.x}"][data-y="${shop.y}"]`);
        shopCell.classList.add('shop');
    });
    checkForDiscount();
}

function moveCharacter(dx, dy) {
    const newX = character.x + dx;
    const newY = character.y + dy;
    if (newX >= 0 && newX < boardSize && newY >= 0 && newY < boardSize) {
        character.x = newX;
        character.y = newY;
        updateBoard();
    }
}

function checkForDiscount() {
    const shop = shops.find(shop => shop.x === character.x && shop.y === character.y);
    if (shop) {
        discountInfo.innerText = `You got a discount: ${shop.discount}!`;
        triggerConfetti();
    } else {
        discountInfo.innerText = '';
    }
}

function triggerConfetti() {
    confetti({
        particleCount: 100,
        spread: 70,
        origin: { y: 0.6 }
    });
}

document.addEventListener('keydown', (event) => {
    switch (event.key) {
        case 'ArrowUp':
            moveCharacter(0, -1);
            break;
        case 'ArrowDown':
            moveCharacter(0, 1);
            break;
        case 'ArrowLeft':
            moveCharacter(-1, 0);
            break;
        case 'ArrowRight':
            moveCharacter(1, 0);
            break;
    }
});

createBoard();