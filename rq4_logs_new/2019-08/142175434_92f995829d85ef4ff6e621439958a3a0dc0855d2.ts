import {config} from '../utils/config';

const PACMAN_RADIUS = config.pacMan.radius;
const OPEN = "open";

function toRadians(deg) {
  return deg * Math.PI / 180
}

function drawPacman(context:CanvasRenderingContext2D, xCoord:number, yCoord:number, state:string) {
    let openAngle = state == OPEN ? toRadians(30): toRadians(5);
    let closeAngle = state == OPEN ? toRadians(330) : toRadians(355);
    context.beginPath();
    context.moveTo(xCoord, yCoord);
    context.arc(xCoord, yCoord, PACMAN_RADIUS, openAngle, closeAngle);
    context.lineTo(xCoord, yCoord);
    context.fillStyle = "yellow";
    context.fill();
    context.closePath();
}

// ctx.clearRect(0, 0, GAME_WIDTH, GAME_HEIGHT);

// ctx.fillStyle = "#f00";
// ctx.fillRect(20, 20, 10, 50);

export {drawPacman};
