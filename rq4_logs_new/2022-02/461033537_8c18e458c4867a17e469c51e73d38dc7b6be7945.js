window.onload = () => {
  const canvas = document.getElementById("canvas");
  const context = canvas.getContext("2d");
  let x = 250,
    y = 150,
    t = Date.now();
  speed = 300;

  let right = document.getElementById("right");
  let left = document.getElementById("left");
  let down = document.getElementById("down");
  let up = document.getElementById("up");
  let dir = 0;

  let coinX = Math.random() * (600 - 50);
  let coinY = Math.random() * (400 - 50);
  let score = 0;

  // Controlling Inputs

  right.onmousedown = () => {
    dir = 1;
  };
  left.onmousedown = () => {
    dir = 2;
  };
  down.onmousedown = () => {
    dir = 3;
  };
  up.onmousedown = () => {
    dir = 4;
  };
  right.onmouseup = () => {
    dir = 0;
  };
  left.onmouseup = () => {
    dir = 0;
  };
  down.onmouseup = () => {
    dir = 0;
  };
  up.onmouseup = () => {
    dir = 0;
  };

  //mobile version
  right.ontouchstart = () => {
    dir = 1;
  };
  right.ontouchend = () => {
    dir = 0;
  };
  // To-Do

  //moving square
  function draw() {
    let timePassed = (Date.now() - t) / 1000;
    t = Date.now(); // set again
    let fps = Math.round(1 / timePassed);

    context.beginPath();
    context.clearRect(0, 0, 600, 400); //clear canvas
    context.font = "25px Arial";
    context.fillStyle = "white";
    context.fillText("Score: " + score, 20, 30);

    context.beginPath();
    context.rect(x, y, 100, 100);
    context.fillStyle = "red";
    context.fill();

    context.beginPath();
    context.rect(coinX, coinY, 50, 50);
    context.fillStyle = "#e3c228";
    context.fill();

    if (dir == 1) {
      if (x + 100 < 600) x += speed * timePassed;
    } else if (dir == 2) {
      if (x > 0) x -= speed * timePassed;
    } else if (dir == 3) {
      if (y + 100 < 400) y += speed * timePassed;
    } else if (dir == 4) {
      if (y > 0) y -= speed * timePassed;
    }

    //Add score
    if (
      coinX <= x + 100 &&
      x <= coinX + 50 &&
      coinY <= y + 100 &&
      y <= coinY + 50
    ) {
      score++;
      coinX = Math.random() * (600 - 50);
      coinY = Math.random() * (400 - 50);
    }

    if (x >= 600) {
      x = -100;
    }
    window.requestAnimationFrame(draw, 16);
  }
  //setInterval(draw, 50);
  draw();
};