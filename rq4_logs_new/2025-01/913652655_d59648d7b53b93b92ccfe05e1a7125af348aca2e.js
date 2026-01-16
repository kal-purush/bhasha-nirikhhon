// Board
let board;
let boardWidth = 480; // Increased width
let boardHeight = 800; // Increased height
let context;

// Bird
let birdWidth = 34;
let birdHeight = 24;
let birdX = boardWidth / 10; // Start the bird closer to the left
let birdY = boardHeight / 2;
let birdImg;

let bird = {
    x: birdX,
    y: birdY,
    width: birdWidth,
    height: birdHeight
};

// Pipes
let pipeArray = [];
let pipeWidth = 80; // Wider pipes
let pipeHeight = 600; // Increased pipe height for the larger game
let pipeX = boardWidth;

let topPipeImg;
let bottomPipeImg;

// Physics
let velocityX = -3; // Faster pipes
let velocityY = 0; // Bird jump speed
let gravity = 0.5; // Slightly stronger gravity

let gameOver = false;
let score = 0;

// Mode variables
let mode = "simple"; // Default mode is "simple"
let wind = 0; // Wind effect
let windDirection = 1; // Wind direction: 1 (right) or -1 (left)
let rainEffectActive = false; // Flag for rain effect

// Sound effects
let jumpSound = new Audio('./sfx_wing.wav');
let pointSound = new Audio('./sfx_point.wav');
let hitSound = new Audio('./sfx_hit.wav');
let dieSound = new Audio('./sfx_die.wav');
let rainSound = new Audio('./rain_sound.mp3');

window.onload = function () {
    displayModeSelection();
};

function displayModeSelection() {
    const modeScreen = document.createElement("div");
    modeScreen.id = "modeScreen";
    modeScreen.style.width = "100%";
    modeScreen.style.height = "100%";
    modeScreen.style.position = "absolute";
    modeScreen.style.background = "rgba(0, 0, 0, 0.8)";
    modeScreen.style.color = "white";
    modeScreen.style.textAlign = "center";
    modeScreen.style.paddingTop = "150px";
    modeScreen.innerHTML = `
        <h1>Flappy Bird</h1>
        <p>Select Mode</p>
        <button id="simpleMode" style="padding: 10px 20px; margin: 20px;">Simple Mode</button>
        <button id="hardMode" style="padding: 10px 20px; margin: 20px;">Rain & Wind Mode</button>
    `;
    document.body.appendChild(modeScreen);

    document.getElementById("simpleMode").onclick = function () {
        mode = "simple";
        startGame();
    };

    document.getElementById("hardMode").onclick = function () {
        mode = "hard";
        startGame();
    };
}

function startGame() {
    const modeScreen = document.getElementById("modeScreen");
    if (modeScreen) modeScreen.remove();

    board = document.getElementById("board");
    board.height = boardHeight;
    board.width = boardWidth;
    context = board.getContext("2d");

    birdImg = new Image();
    birdImg.src = "./flappybird.png";

    topPipeImg = new Image();
    topPipeImg.src = "./toppipe.png";

    bottomPipeImg = new Image();
    bottomPipeImg.src = "./bottompipe.png";

    if (mode === "hard") {
        rainEffectActive = true;
        board.style.backgroundImage = "url('./rainy_background.png')";
        rainSound.loop = true;
        rainSound.play();
    } else {
        board.style.backgroundImage = "url('./flappybirdbg.png')";
    }

    requestAnimationFrame(update);
    setInterval(placePipes, 1800); // Pipes every 1.8 seconds
    document.addEventListener("keydown", moveBird);
}

function update() {
    requestAnimationFrame(update);
    if (gameOver) return;
    context.clearRect(0, 0, board.width, board.height);

    if (mode === "hard") {
        applyRainEffect();
        applyWindEffect();
    }

    velocityY += gravity;
    bird.y = Math.max(bird.y + velocityY, 0);
    bird.x = Math.max(0, Math.min(boardWidth / 3, bird.x + wind)); // Limit bird to the left third of the screen
    context.drawImage(birdImg, bird.x, bird.y, bird.width, bird.height);

    if (bird.y > boardHeight) {
        gameOver = true;
        dieSound.play();
    }

    for (let i = 0; i < pipeArray.length; i++) {
        let pipe = pipeArray[i];
        pipe.x += velocityX;
        context.drawImage(pipe.img, pipe.x, pipe.y, pipe.width, pipe.height);

        if (!pipe.passed && bird.x > pipe.x + pipe.width) {
            score += 0.5;
            pipe.passed = true;
            pointSound.play();
        }

        if (detectCollision(bird, pipe)) {
            gameOver = true;
            hitSound.play();
        }
    }

    while (pipeArray.length > 0 && pipeArray[0].x < -pipeWidth) {
        pipeArray.shift();
    }

    context.fillStyle = "white";
    context.font = "45px sans-serif";
    context.fillText(score, 5, 45);

    if (gameOver) {
        context.fillText("GAME OVER", boardWidth / 4, boardHeight / 2);
    }
}

function placePipes() {
    if (gameOver) return;

    let pipeGap = board.height / 4; // Consistent gap between pipes
    let randomPipeY = -pipeHeight / 4 - Math.random() * (pipeHeight / 2); // Generate a balanced random Y position for the top pipe
    let openingSpace = board.height / 4; // Space for the bird to pass through

    let topPipe = {
        img: topPipeImg,
        x: pipeX + 100, // Start pipes slightly off-screen for smoother transition
        y: randomPipeY,
        width: pipeWidth,
        height: pipeHeight,
        passed: false
    };
    pipeArray.push(topPipe);

    let bottomPipe = {
        img: bottomPipeImg,
        x: pipeX + 100, // Same offset as the top pipe
        y: randomPipeY + pipeHeight + openingSpace,
        width: pipeWidth,
        height: pipeHeight,
        passed: false
    };
    pipeArray.push(bottomPipe);
}

function moveBird(e) {
    if (e.code === "Space" || e.code === "ArrowUp" || e.code === "KeyX") {
        velocityY = -7; // Slightly stronger jump
        jumpSound.play();

        if (gameOver) {
            bird.y = birdY;
            pipeArray = [];
            score = 0;
            velocityX = -3;
            gravity = 0.5;
            wind = 0;
            gameOver = false;
        }
    }
}

function detectCollision(a, b) {
    return (
        a.x < b.x + b.width &&
        a.x + a.width > b.x &&
        a.y < b.y + b.height &&
        a.y + a.height > b.y
    );
}

function applyRainEffect() {
    context.fillStyle = "rgba(173, 216, 230, 0.1)";
    context.fillRect(0, 0, boardWidth, boardHeight);
}

function applyWindEffect() {
    if (Math.random() < 0.01) { // Wind changes direction less frequently
        windDirection = Math.random() < 0.5 ? -1 : 1;
        wind = windDirection * (Math.random() * 1.5); // Wind magnitude: 0 to 1.5
    }
}