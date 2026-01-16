import config from '../constants/config';
import { PACMAN_STATE } from '../constants/states';
import { toRadians } from '../utils';

const PACMAN_RADIUS = config.pacMan.radius;

// TODO: move the pacman logic to its own class, which will track current state and collisions
function drawPacman(context:CanvasRenderingContext2D, xCoord:number, yCoord:number, state:PACMAN_STATE) {
    let openAngle = state === PACMAN_STATE.OPEN ? toRadians(30): toRadians(5);
    let closeAngle = state === PACMAN_STATE.OPEN ? toRadians(330) : toRadians(355);
    context.beginPath();
    context.moveTo(xCoord, yCoord);
    context.arc(xCoord, yCoord, PACMAN_RADIUS, openAngle, closeAngle);
    context.lineTo(xCoord, yCoord);
    context.fillStyle = "yellow";
    context.fill();
    context.closePath();
}

export {drawPacman};
