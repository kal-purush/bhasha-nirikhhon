import Game from "../src/lib/game";
import { GAME_STATE, PACMAN_STATE } from '../src/constants/states';
import { expect } from 'chai';
import config from "../src/constants/config";

const PACMAN_RADIUS = config.pacMan.radius;

import "mocha";

function defaultGame() {
  return new Game(200, 400);
}

describe("Game", function() {

    it("should check if the game is instantiated", () => {
      expect(defaultGame().gameWidth).to.equal(200);
      expect(defaultGame().gameHeight).to.equal(400);
	});
	
	it.skip("should check the PacMan state after game start", () => {
		let testGame = defaultGame();
		testGame.start();
		// The below lines should be mocked with testdouble as the pacman object is not part of this unit test
		// expect(testGame.pacman.size).to.equal({x: PACMAN_RADIUS, y: PACMAN_RADIUS});
		// expect(testGame.pacman.pacmanState).to.equal(PACMAN_STATE);
	});

	it.skip("should update the PacMan position", () => {
		// The testdouble library should mock pacman and check flow
	});

	it.skip("should draw PacMan on the canvas", () => {
		// A placeholder for the time when we do canvas testing
	});

	it.skip("should pause and unpause the game", () => {
		// First opportunity for TDD
	});
});



