function ballsColider(balls) {
    let i = 0;
    let ball = balls.shift();
    if (balls.length === 0) {
        return;
    }
    while (i < balls.length - 1) {
        if ((Math.abs(ball.figure.point.x) - Math.abs(balls[i].figure.point.x)) < ball.figure.rad) {
            ball.collisioned = true;
            ball[i].collisioned = true;
        }
        i++;
    }
    ballsColider(balls);
}