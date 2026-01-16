const canvas = document.getElementById('gameCanvas');
const ctx = canvas.getContext('2d');

canvas.width = window.innerWidth;
canvas.height = window.innerHeight;

const carImage = new Image();
carImage.src = 'redCar1.png'; // Replace with the path to your car image

const background = new Image();
background.src = 'floor.jpg'; // Replace with the path to your background image

const obstacleImage1 = new Image();
obstacleImage1.src = 'pencil.png'; // Replace with the path to your first obstacle image

const obstacleImage2 = new Image();
obstacleImage2.src = 'coins.png'; // Replace with the path to your second obstacle image

const finishLineImage = new Image();
finishLineImage.src = 'finish.png'; // Replace with the path to your second obstacle image


let car = {
    x: 100,
    y: canvas.height / 2,
    width: 150,
    height: 70,
    speed: 5,
    dy: 0
};

let obstacles = [];
let baseSpeed = 4;
let speedFactor = 2.5;
let isSlowedDown = false;
let bgX = 0;
let tilesPassed = 0;
let startTime;
let gameOver = false;

function drawBackground() {
    ctx.drawImage(background, bgX, 0, canvas.width, canvas.height);
    ctx.drawImage(background, bgX + canvas.width, 0, canvas.width, canvas.height);

    bgX -= baseSpeed * speedFactor;
    if (bgX <= -canvas.width) {
        bgX = 0;
        tilesPassed++;
    }
}

function drawCar() {
    ctx.drawImage(carImage, car.x, car.y, car.width, car.height);
}

function drawObstacles() {
    obstacles.forEach(obstacle => {
        let obstacleImage;
        if (obstacle.type === 'finish') {
            obstacleImage = finishLineImage;
        } else {
            obstacleImage = obstacle.type === 1 ? obstacleImage1 : obstacleImage2;
        }
        ctx.drawImage(obstacleImage, obstacle.x, obstacle.y, obstacle.width, obstacle.height);
    });
}

function updateObstacles() {
    obstacles.forEach(obstacle => {
        obstacle.x -= baseSpeed * speedFactor;
    });

    if (obstacles.length && obstacles[0].x < -obstacles[0].width) {
        obstacles.shift();
    }

    if (tilesPassed >= 10 && !obstacles.some(ob => ob.type === 'finish')) {
        obstacles.push({
            x: canvas.width,
            y: 0,
            width: 50,
            height: canvas.height,
            type: 'finish'
        });
    }

    if (Math.random() < 0.02 && tilesPassed < 10) {
        let type = Math.random() < 0.5 ? 1 : 2;
        obstacles.push({
            x: canvas.width,
            y: Math.random() * (canvas.height - 60),
            width: type === 1 ? 30 : 100,
            height: type === 1 ? 350 : 100,
            type: type
        });
    }
}

function detectCollision() {
    for (let i = 0; i < obstacles.length; i++) {
        const obstacle = obstacles[i];
        if (car.x < obstacle.x + obstacle.width &&
            car.x + car.width > obstacle.x &&
            car.y < obstacle.y + obstacle.height &&
            car.y + car.height > obstacle.y) {
            if (obstacle.type === 'finish') {
                gameOver = true;
                const timeTaken = (Date.now() - startTime) / 1000;
                displayGameOverMessage(timeTaken);
                return;
            } else {
                isSlowedDown = true;
                return;
            }
        }
    }
    isSlowedDown = false;
}

function displayGameOverMessage(timeTaken) {
    ctx.fillStyle = 'black';
    ctx.font = '48px serif';
    ctx.fillText(`Time: ${timeTaken.toFixed(2)} seconds`, canvas.width / 2 - 150, canvas.height / 2 - 50);
    ctx.fillText('Press Space to Play Again', canvas.width / 2 - 200, canvas.height / 2 + 50);
}


function drawTimer() {
    const currentTime = (Date.now() - startTime) / 1000;
    ctx.fillStyle = 'black';
    ctx.font = '24px serif';
    ctx.fillText(`Time: ${currentTime.toFixed(2)} seconds`, 10, 30);
}

function update() {
    if (gameOver) return;

    ctx.clearRect(0, 0, canvas.width, canvas.height);

    drawBackground();
    car.y += car.dy;
    if (car.y < 0) car.y = 0;
    if (car.y + car.height > canvas.height) car.y = canvas.height - car.height;

    updateObstacles();
    drawObstacles();
    drawCar();
    drawTimer();
    detectCollision();

    speedFactor = isSlowedDown ? 0.4 : 2.5;

    requestAnimationFrame(update);
}

function restartGame() {
    car = {
        x: 100,
        y: canvas.height / 2,
        width: 150,
        height: 70,
        speed: 5,
        dy: 0
    };
    obstacles = [];
    baseSpeed = 4;
    speedFactor = 2.5;
    isSlowedDown = false;
    bgX = 0;
    tilesPassed = 0;
    gameOver = false;
    startTime = Date.now();
    update();
}

window.addEventListener('keydown', (e) => {
    if (e.key === 'ArrowUp') car.dy = -car.speed;
    if (e.key === 'ArrowDown') car.dy = car.speed;
    if (e.key === ' ') {
        if (gameOver) restartGame();
    }
});

window.addEventListener('keyup', (e) => {
    if (e.key === 'ArrowUp' || e.key === 'ArrowDown') car.dy = 0;
});

background.onload = function() {
    startTime = Date.now();
    update();
};