import Game from '../game';
import config from '../../constants/config';
import { PACMAN_STATE } from '../../constants/states';
import { ObjectBounds } from '../../types';
import { getObjectBounds, toRadians } from '../../utils';
import BaseSprite from './baseSprite';

const PACMAN_RADIUS = config.pacMan.radius;

export default class Pacman extends BaseSprite {
	pacmanState: PACMAN_STATE;

	constructor(game: Game) {
		super(game);
		this.size = { x: PACMAN_RADIUS, y: PACMAN_RADIUS };
		this.bounds = getObjectBounds(this.position, this.size);
		this.pacmanState = PACMAN_STATE.OPEN;
	}

	reset() {
		this.position = {
			x: this.gameWidth / 2.0,
			y: this.gameHeight / 2.0
		}
		this.speed = {
			x: 2.0,
			y: 0.0
		}
	}

	draw(context: CanvasRenderingContext2D): void {
		const { x, y } = this.position;
		let openAngle = this.pacmanState === PACMAN_STATE.OPEN ? toRadians(30) : toRadians(5);
		let closeAngle = this.pacmanState === PACMAN_STATE.OPEN ? toRadians(330) : toRadians(355);
		context.beginPath();
		context.moveTo(x, y);
		context.arc(x, y, PACMAN_RADIUS, openAngle, closeAngle);
		context.lineTo(x, y);
		context.fillStyle = "yellow";
		context.fill();
		context.closePath();
	}

	update(deltaTime: number): void {
		const { x, y } = this.position;
		this.position = {
			x: x + this.speed.x,
			y: y + this.speed.y
		}
	}

	detectCollision(otherBounds: ObjectBounds): boolean {
		return false;
	}

	detectWallCollision(): boolean {
		return false;
	}



}