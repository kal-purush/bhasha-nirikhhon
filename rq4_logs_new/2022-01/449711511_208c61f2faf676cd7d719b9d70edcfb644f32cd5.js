 class Actor {
	constructor(image, x, y, isPlayer) {
		this.x = x;
		this.y = y;
		this.dead = false;
		this.isPlayer = isPlayer;
		this.loaded = false;
		this.image = new Image();
		let obj = this;
		this.image.addEventListener("load", function () {
      obj.loaded = true;
    }
    );
		this.image.src = image;
    this.w = this.x;
    this.h = this.y;
	}

  touch(actor) {
		let touch = false;
    if (this.x === actor.x && this.y === actor.y) {
				touch = true;
		}
    return touch;
  }
}

class Player extends Actor {
  constructor(image, x, y, isPlayer, dead) {
    super(image, x, y, isPlayer, dead);
  }

  move(v, d) {
		if(v == "x") {
		  d *= 2;
			this.x += d;
			if(this.x + this.image.width * scale > canvas.width) {
				this.x -= d;
			}
			if(this.x < 0) {
				this.x = 0;
			}
    }

    if(v == "y") {
      d *= 2;
      this.y += d;
      if(this.y + this.image.height * scale > canvas.height) {
      this.y -= d;
      }
      if(this.y < 0) {
      this.y = 0;
      }
    }
	}
}

class Enemy extends Actor {
  constructor(image, x, y, isPlayer) {
    super(image, x, y, isPlayer);
  }

  approach_XPlus(actor) {
	  let gravity_XPlus = false;
    if (this.x < actor.x) {
      gravity_XPlus = true;
    }
    return gravity_XPlus;
  }

  approach_XMinus (actor) {
	  let gravity_XMinus = false;
    if (this.x > actor.x) {
      gravity_XMinus = true;
    }
    return gravity_XMinus;
  }

  approach_YMPlus(actor) {
		let gravity_YPlus = false;
    if (this.y < actor.y) {
      gravity_YPlus = true;
    }
    return gravity_YPlus;
  }

  approach_YMinus(actor) {
		let gravity_YMinus = false;
    if (this.y > actor.y) {
      gravity_YMinus = true;
    }
    return gravity_YMinus;
  }

  move(v, d) {
		if(v == "x") {
		  d *= 2;
			this.x += d;
    }

    if(v == "y") {
      d *= 2;
      this.y += d;
    }
	}
}

class Heart extends Actor {
  constructor(image, x, y, isPlayer) {
    super(image, x, y, isPlayer);
  }

  takePositionX() {
    let posX = this.x;
    return posX;
  }

  takePositionY() {
    let posY = this.y;
    return posY;
  }
}

class FlyObject extends Actor {
  constructor(x, y, isPlayer, dead) {
    super(dead);
    this.isPlayer = isPlayer;
    this.x = x;
    this.y = y;
    this.hide = 1;
  }

  updateActor() {
    if(this.y < -30) this.dead = true;
  }

  move(d) {
    d *= 1;
    this.y += d;
	}

  takePositionX() {
    let posX = this.x;
    return posX;
  }

  takePositionY() {
    let posY = this.y;
    return posY;
  }
}

const canvas = document.querySelector("#canvas");
const ctx = canvas.getContext("2d");
canvas.width = window.innerWidth;
canvas.height = window.innerHeight;
const scale = 0.4;
const step = 20;
const blockSize = 16;
const blockRowCount = Math.trunc((canvas.width / scale) / 100);
const blockColumnCount = Math.trunc((canvas.height / scale) / 100);
const horizontalCorridor = Math.trunc(blockRowCount / 2);
const verticalCorridor = Math.trunc(blockColumnCount / 2);
const randomInteger = (value) =>  Math.trunc(Math.random() * value);
const positionFlyScoreX = (position) => posX = position.takePositionX();
const positionFlyScoreY = (position) => posY = position.takePositionY();
const cathAudio = new Audio('./sound/cath.wav');
const takenAudio = new Audio('./sound/take.wav');
const stepAudio = new Audio('./sound/step.wav');
let score = 0;
let flyScore = '';
let flyObjectText = document.querySelector('#flyText');
let heartCollected = document.querySelector('#totalScore');
let newStart = document.querySelector('#newGame');
let hideScore = null;

const enemy = new Enemy("./images/skull_2.png", (blockRowCount - 1) * (blockSize / scale), verticalCorridor * (blockSize / scale), "enemy");

const player = new Player("./images/smile_green.png", horizontalCorridor * (blockSize / scale), verticalCorridor * (blockSize / scale), "player");

const heart = [new Heart()];

const flyObject = [];

function startGame() {
  if (!player.dead) {
    playGame();
  } else {
    introGame(collection);
  }
}

canvas.addEventListener("contextmenu", function (e) {
  e.preventDefault(); return false;
  }
);
document.addEventListener("keydown", function (e) {
  keyDown(e);
  }
);

startGame();

function introGame(collection) {
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  heartCollected.style.left = 0 + 'px';
  heartCollected.style.top = 0 + 'px';
  heartCollected.innerHTML = collection;
  document.body.removeChild(flyObjectText);
  document.body.append(heartCollected);
  newGame();
}

function playGame() {
  requestAnimationFrame(update);
}

delay();

function newGame() {
  document.body.append(newStart);
  document.addEventListener('DOMContentLoaded', function() {
    const gameAgaine = document.getElementById('btnGame')
    gameAgaine.addEventListener('button', playGame)
  })
}

function update() {
  for (let i = 0; i < heart.length; i++) {
    let touch = heart[i].touch(player);
    if (touch) {
      score++;
      takenAudio.play();
      flyScore = score.toString();
      let heartPosition = heart[i];
      flyObject.push(new FlyObject(positionFlyScoreX(heartPosition), positionFlyScoreY(heartPosition), flyScore));
      hideScore = 1;
      heart.splice(i, 1);
    }
  };

  for (let i = 0; i < flyObject.length; i++) {
    flyObject[i].updateActor();
    if (flyObject[i].dead === true) {
      flyObject.splice(i, 1);
    }
  };

  if (heart.length-1 < 5) {
    heart.push(new Heart("./images/heart_100_100.png", randomInteger(blockRowCount) * (blockSize / scale), randomInteger(blockColumnCount) * (blockSize / scale), "heart"));
  }

  moveFlyObject();

	draw();

  touch = enemy.touch(player);
  if (touch) {
    cathAudio.play();
    player.dead = true;
    cancelAnimationFrame(update);
    return introGame(`Collection of hearts: ${flyScore} <br/> F5 - new game`);
  }
  
  requestAnimationFrame(update);
}

function draw() {
	ctx.clearRect(0, 0, canvas.width, canvas.height);
  ctx.fillStyle = "Black";
  ctx.fillRect(0, 0, canvas.width, canvas.height);

  for (let i = 0; i < heart.length; i++) {
    if (heart[i].isPlayer === "heart") {
      drawElements(heart[i]);
    }
  }

	drawElements(player);

  drawElements(enemy);

  for (let i = 0; i < flyObject.length; i++) {
    let flyTextCount = flyObject[i].isPlayer;
    drawFlyObject(flyObject[i], flyTextCount);
  }
}

function drawElements(actor) {
	ctx.drawImage (
		actor.image,
		0,
		0,
		actor.image.width,
		actor.image.height,
		actor.x,
		actor.y,
		actor.image.width * scale,
		actor.image.height * scale
	);
}

function drawFlyObject(flyingObject, flyTextCount) {
  if (hideScore !== 0) hideScore -= .01;
  flyObjectText.style.left = positionFlyScoreX(flyingObject) + 'px';
  flyObjectText.style.top = positionFlyScoreY(flyingObject) + 'px';
  flyObjectText.style.opacity = hideScore;
  flyObjectText.innerHTML = flyTextCount;
  document.body.append(flyObjectText);
}

function keyDown(e) {
  stepAudio.play();
	switch(e.keyCode) {
		case 37:
			player.move("x", -step);
			break;
		case 39:
			player.move("x", step);
			break;
		case 38:
			player.move("y", -step);
			break;
		case 40:
			player.move("y", step);
			break;
	}
}

function delay() {
  setTimeout(function (){
    moveEnemy();
    delay();
  }, 400);
};

function moveEnemy() {
  let gravity_XMinus = enemy.approach_XMinus(player);
  let gravity_XPlus = enemy.approach_XPlus(player);
  let gravity_YPlus = enemy.approach_YMPlus(player);
  let gravity_YMinus = enemy.approach_YMinus(player);

  if (gravity_XMinus) {
    enemy.move("x", -step);
  }
  if (gravity_XPlus) {
    enemy.move("x", step);
  }
  if (gravity_YMinus) {
    enemy.move("y", -step);
  }
  if (gravity_YPlus) {
    enemy.move("y", step);
  }
  return enemy;
}

function moveFlyObject() {
  for (let i = 0; i < flyObject.length; i++) {
    flyObject[i].move(-3);
  };
}