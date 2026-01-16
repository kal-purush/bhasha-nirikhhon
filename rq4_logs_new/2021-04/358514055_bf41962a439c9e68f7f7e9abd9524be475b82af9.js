class Ball {
  constructor(game) {
    const thisBall = this;

    thisBall.game = game;
    thisBall.gameWidth = game.gameWidth;
    thisBall.gameHeight = game.gameHeight;
    thisBall.position = {
      x: Math.floor(Math.random() * 750) + 1,

      y: Math.floor(Math.random() * 550) + 1,
    };
    thisBall.radius = 20;
    thisBall.color = '#960000';
  }

  draw(context) {
    const thisBall = this;

    context.fillStyle = thisBall.color;
    context.beginPath();
    context.arc(
      thisBall.position.x,
      thisBall.position.y,
      thisBall.radius,
      0,
      Math.PI * 2
    );
    context.fill();
    context.stroke();
  }

  update(deltaTime) {}
}

export default Ball;