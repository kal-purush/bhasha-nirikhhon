/// <reference path="Definitions/react-0.13.0.d.ts" />
import React = require('react');
import Scene = require('./Components/Scene');
import art = require('./art');

export class Game {
	_intervalID: number;
	_fps = 5;
	_state = { entities: [{ name: 'Game', art: art.noughts }] }
	_contentElement = document.getElementById('content');

	_createPlayer() {
		return { name: 'Player', art: art.dude }
	}

	_update() {
		if (this._state.entities.length < 3) {
			this._state.entities.push(this._createPlayer());
		} else {
			var x = this._state.entities.shift()
			this._state.entities.push(x);
		}
	}

	_draw() {
		React.render(React.createElement(Scene, this._state), this._contentElement);
	}

	start() {
		var skipTicks = 1000 / this._fps;
		var maxFrameSkip = 10;
		var nextGameTick = (new Date).getTime();

		var run = () => {
			var loops = 0;

			while ((new Date).getTime() > nextGameTick && loops < maxFrameSkip) {
				this._update();
				nextGameTick += skipTicks;
				loops++;
			}

			this._draw();
		}

		this._intervalID = setInterval(run, 1000 / this._fps);
	}

	stop() {
		clearInterval(this._intervalID)
	}
}




