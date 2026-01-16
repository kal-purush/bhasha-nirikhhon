import { Duck } from '../Types/Duck';
import { GAME_CONSTANTS } from '../Constant/constant';

export function applyBoundaryConstraints(duck: Duck): void {
    const { DUCK } = GAME_CONSTANTS;

    if (duck.position.left >= DUCK.MAX_LEFT) {
        duck.position.left = DUCK.MAX_LEFT - 0.1;
        duck.direction.x *= -1;
    }
    if (duck.position.left <= DUCK.MIN_LEFT) {
        duck.position.left = DUCK.MIN_LEFT + 0.1;
        duck.direction.x *= -1;
    }
    if (duck.position.top >= DUCK.MAX_TOP) {
        duck.position.top = DUCK.MAX_TOP - 0.1;
        duck.direction.y *= -1;
    }
    if (duck.position.top <= DUCK.MIN_TOP) {
        duck.position.top = DUCK.MIN_TOP + 0.1;
        duck.direction.y *= -1;
    }
}