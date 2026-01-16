import {GAME_STATE} from '../constants/states';

export default class Game {
	gameWidth: number;
	gameHeight: number;
	gameState: GAME_STATE;
	currentLevel: number;


  constructor(gameWidth, gameHeight) {
    this.gameWidth = gameWidth;
    this.gameHeight = gameHeight;
    this.gameState = GAME_STATE.RUNNING;
    
    this.currentLevel = 0;
  }

  start() {
		// setting things up when game is starting
  }

  update(deltaTime) {
  }

  draw(ctx) {
    // central loop where all game objects are drawn
  }

  togglePause() {
    // allow for game to be paused
  }
}